# encoding: utf-8

require "trello_utils"
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "json"
require "time"
require "set"
require "active_support/core_ext/string/inflections"
# ------------------------------------------------------------------------------

# The trello filter is used querying the trello database and returning the resulting
# cards as events

class LogStash::Inputs::Trello < LogStash::Inputs::Base
	config_name "trello"
	milestone 1

	@@plural_entities = [
		"actions",
		"boards",
		"cards",
		"checklists",
		"invitations",
		"labels",
		"lists",
		"members",
		"memberships",
		"organizations",
		"powerups",
		"checkItemStates",
		"entities",
		"checkItems"
	]
		
	@@singular_entities = [
		"action",
		"board",
		"card",
		"checklist",
		"invitation",
		"label",
		"list",
		"member",
		"membership",
		"organization",
		"powerup",
		"checkItem"
	]

	@@all_entities = @@singular_entities + @@plural_entities

	@@plural_ids = [
		"idChecklists",
		"idLabels",
		"idMembers",
		"idMembersVoted"
	]

	@@singular_ids = [
		"idBoard",
		"idList",
		"idCard",
		"idMemberCreator",
		"idMember",
		"idOrganization",
		"idCheckItem",
		"idShort"
	]

	@@all_ids = @@singular_ids + @@plural_ids
	# --------------------------------------------------------------------------

	config(:fields, :validate => :array, :default => ["default"])
	config(:filters, :validate => :hash, :default => {})
	config(:entities, :validate => :array, :default => ["default"])

	default(:codec, "json_lines")
		# defualt: json_lines

	config(:port, :validate => :number, :default => 443)
		# The port trello listens on for REST requests

	config(:organizations, :validate => :array, :required => true)
		# an array of organizations from which to derive board ids

	config(:snake_case, :validate => :boolean, :default => false)
		# coerce output field names into snake_case

	config(:interval, :validate => :number, :default => 1800)
		# query interval
		# default: 30 minutes

	config(:key, :validate => :string, :required => true)
		# oauth key

	config(:token, :validate => :string, :required => true)
		# oauth secret

	config(:organizations, :validate => :array, :required => true)
		# an array of organizations from which to derive board ids

	config(:board_ids, :validate => :array, :default => [])
		# ids of boards to be parsed

	config(:output_types, :validate => :array, :default => ["all"])
	# --------------------------------------------------------------------------

	public
	def register()
		if @fields == ["all"]
			@fields = TrelloUtils::PARAM_ALL_FIELDS
		elsif @fields == ["default"]
			@fields = TrelloUtils::PARAM_DEFAULT_FIELDS
		end

		if @filters == ["all"]
			@filters = TrelloUtils::PARAM_ALL_FILTERS
		elsif @filters == ["default"]
			@filters = TrelloUtils::PARAM_DEFAULT_FILTERS
		end
		
		if @entities == ["all"]
			@entities = TrelloUtils::PARAM_ALL_ENTITIES
		elsif @entities == ["default"]
			@entities = TrelloUtils::PARAM_DEFAULT_ENTITIES
		end

		if @output_type == ["all"]
			@output_types = [
						"board",
						"memberships",
						"labels",
						"cards",
						"lists",
						"members",
						"checklists",
						"actions"
			]
		end

		@client = TrelloUtils::TrelloClient.new({
				organizations: @organizations,
				key:           @key,
				token:         @token,
				board_ids:     @board_ids,
				fields:        @fields,
				entities:      @entities,
				filters:       @filters,
				port:          @port
		})
	end
	# --------------------------------------------------------------------------
	
	private
	def create_lut(data)
		# This cannot be done recursively because a recursive func will pick up
		# stubs
		lut = {}
		@@plural_entities.map { |entity| lut[entity] = {} }
		
		data.each do |key, entities|
			if lut.has_key?(key)
				entities.each do |entity|
					lut[key][ entity["id"] ] = entity
				end
			end
		end
		return lut
	end

	private
	def recurse(data, hash_func, func=nil)
		if func.nil?
			func = lambda { |store, key, val| return val }
		end

		store = {}
		def _recurse(data, store, hash_func, func)
			data.each do |key, val|
				if val.is_a?(Hash)
					# logic goes here
					result = hash_func.call(store, key, val)
					if result
						store[key] = result
					else
						store[key] = _recurse(val, store, hash_func, func)
					end
				else
					store[key] = func.call(store, key, val)
				end
			end
		end
		_recurse(data, store, hash_func, func)
		return store
	end

	private
	def conform_field_names(data, plural=false)
		if not data.is_a?(Hash)
			return data
		end
		data = data.clone
		data.to_a.each do |key, val|
			if @@all_ids.include?(key)
				# clobber non-id fields with id fields
				new_key = key.gsub(/^id/, '')
				new_key = new_key[0].downcase + new_key[1..-1]
				if plural
					new_key = new_key.pluralize
				end
				data[new_key] = val
				data.delete(key)
			end
		end
		data = conform_subfield_names(data)
		return data
	end

	private
	def conform_subfield_names(data, plural=false)
		func = lambda do |store, key, val|
			if val.is_a?(Hash)
				output = val
				val.to_a.each do |k, v|
					if @@all_ids.include?(k)
						# clobber non-id fields with id fields
						new_key = k.gsub(/^id/, '')
						new_key = new_key[0].downcase + new_key[1..-1]
						if plural
							new_key = new_key.pluralize
						end
						output[new_key] = v
						output.delete(k)
					end
				end
				return output
			else
				return val
			end
		end
		return recurse(data, func, func)
	end
	
	private
	def clean_data(data)
		data = data.clone
		# remove actions data field
		if data.has_key?("data")
			data["data"].each do |key, val|
				if not key == "old"
					data[key] = val
				end
			end
			data.delete("data")
		end
		if data.has_key?("board")
			data.delete("board")
		end
		return data
	end
	
	private
	def get_data_structure_type(val)
		if val.is_a?(Array)
			if not val.empty?
				if val[0].is_a?(Hash)
					return "array_of_hashes"
				elsif val[0].is_a?(String)
					return "array_of_strings"
				end
			else
				return "empty_array"
			end
		elsif val.is_a?(Hash)
			if val.has_key?("id")
				return "hash_with_id"
			else
				return "hash_without_id"
			end
		else
			return val.class.to_s
		end
	end

	private
	def expand_entities(data, lut)
		func = lambda do |store, key, val|
			pkey = key.pluralize
			l = lut[pkey]
			if l.nil?
				l = {}
			end

			output = val
			ds_type = get_data_structure_type(val)
			if /^array_of/.match(ds_type)
				output = []
				if ds_type == "array_of_hashes"
					val.each do |item|
						item_type = get_data_structure_type(item)
						new_item = item
						if item_type == "hash_with_id"
							if l.has_key?(item["id"])
								new_item = l[item["id"]]
							end
						end
						output.push(new_item)
					end

				elsif ds_type == "array_of_strings"
					val.each do |id|
						if l.has_key?(id)
							output.push(l[id])
						end
					end
				end
				output = group(output)

			elsif ds_type == "empty_array"
				output = nil

			elsif ds_type == "hash_with_id"
				if l.has_key?(val["id"])
					output = l[val["id"]]
				end

			elsif ds_type == "String"
				if l.has_key?(val)
					output = l[val]
				end
			end

			return conform_field_names(output)
		end
		return recurse(data, func, func)
	end
	
	private
	def collapse(data, source, entities)
		# entities might be drawn from lut.keys()
		output = { source => {} }
		data.each do |key, val|
			if entities.include?(key)
				output[key] = val
			else
				output[source][key] = val
			end
		end
		return output
	end

	private
	def coerce_nulls(data)
		data.each do |index, item|
			if item == ""
				item == nil
			end
		end
		return data
	end

	private
	def group(data)
		def _group(data)
			prototype = {}
			data.each do |entry|
				entry.each do |key, val|
					prototype[key] = []
				end
			end
			data.each do |entry|
				entry.each do |key, val|
					if val != "" and !val.nil?
						prototype[key].push(val)
					end
				end
			end
			return prototype
		end

		# ensure the proper data structure
		if data.is_a?(Array)
			if not data.empty?
				if data[0].is_a?(Hash)
					return _group(data)
				end
			end
		else
			# return data if data structure is wrong
			return data
		end
	end

	private
	def to_snake_case(data)
		data = data.clone
		data.each { |index, item| index.map! { |item| item.underscore } }
		return data
	end

	private
	def nested_hash_to_matrix(data)
		@sep = '.'
		@output = []
		def _nested_hash_to_matrix(data, name)
			data.each do |key, val|
				new_key = name + @sep + key.to_s
				if val.is_a?(Hash) and val != {}
					_nested_hash_to_matrix(val, new_key)
				else
					@output.push([new_key, val])
				end
			end
			return @output
		end

		@output = _nested_hash_to_matrix(data, @sep)
		@output = @output.map { |key, val| [key.split('.')[2..-1], val] }
		return @output
	end

	private
	def matrix_to_nested_hash(data)
		output = {}
		data.each do |keys, value|
			cursor = output
			for key in keys[0..-2]
				if !cursor.include?(key)
					cursor[key] = {}
					cursor = cursor[key]
				else
					cursor = cursor[key]
				end
			end
			cursor[keys[-1]] = value
		end
		return output
	end
	# --------------------------------------------------------------------------

	private
	def process_response(response, queue)
		timestamp = Time.now

		response = collapse(response, "board", @@plural_entities)
		lut = create_lut(response)

		@output_types.each do |out_type|
			if response.has_key?(out_type)
				response[out_type].each do |source|
					singular = out_type.singularize
					data = clean_data(source)
					data = conform_field_names(data, true)
					data = expand_entities(data, lut)
					all_ent = @@all_entities.clone
					all_ent.delete("entities")
					data = collapse(data, singular, all_ent)

					# shuffle board info into data
					board = response["board"]
					board = conform_field_names(board)
					data["board"] = board

					data = nested_hash_to_matrix(data)
					data = coerce_nulls(data)
					if @snake_case
						data = to_snake_case(data)
					end
					data = matrix_to_nested_hash(data)
					event = nil
					# set the timestamp of actions to their date field
					_timestamp = timestamp
					if singular == "action"
						_timestamp = data["action"]["date"]
						data["action"].delete("date")
					end
					event = LogStash::Event.new(
						"host" => @host, 
						"type" => @type + '_' + singular,
						"@timestamp" => _timestamp,
						"message" => JSON.dump(source) )
					data.each do |key, val|
						event[key] = val
					end
					decorate(event)
					queue << event
				end
			end
		end
	end

	public
	def run(queue)
		init_time = Time.now - @interval
		init_time = init_time.strftime('%Y-%m-%dT%H:%M:%S%z')
		query_times = {}
		board_ids.each { |board_id| query_times[board_id] = init_time}
		Stud.interval(@interval) do
			@client.board_ids.each do |board_id|
				uri = @client.get_uri(board_id, query_times[board_id])
				response = nil
				begin
					response = @client.issue_request(uri)
				rescue RuntimeError
					next
				end
				process_response(response, queue)
				query_times[board_id] = Time.now.strftime('%Y-%m-%dT%H:%M:%S%z')
			end
		end
	end
end