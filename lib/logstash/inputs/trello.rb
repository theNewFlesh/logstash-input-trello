# encoding: utf-8

require "trello_utils"
require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket"
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
	@all_entities_and_ids = @@all_entities + @@all_ids
	# --------------------------------------------------------------------------

	# An array of all the fields configurable via the trello api.
	# Unincluded fields will not show up in event data.
	# Options are:
	# - ["default"] = a custom array of fields defined within the plugin
	# - ["all"] = all field
	# - An array containg any of these fields:
	# - - actions_entities
	# - - action_member
	# - - action_memberCreator
	# - - card_stickers
	# - - memberships_member
	# - - organization
	# - - myPrefs
	# - - card_attachments
	# - - cards
	# - - card_checklists
	# - - boardStars
	# - - labels
	# - - lists
	# - - members
	# - - membersInvited
	# - - checklist
	#
	# -	- active
	# -	- addAttachmentToCard
	# -	- addChecklistToCard
	# -	- addMemberToBoard
	# -	- addMemberToCard
	# -	- addMemberToOrganization
	# -	- addToOrganizationBoard
	# -	- admin
	# -	- admins
	# -	- avatarHash
	# -	- badges
	# -	- billableMemberCount
	# -	- bio
	# -	- bioData
	# -	- bytes
	# -	- checkItemStates
	# -	- closed
	# -	- color
	# -	- commentCard
	# -	- confirmed
	# -	- convertToCardFromCheckItem
	# -	- copyBoard
	# -	- copyCard
	# -	- copyCommentCard
	# -	- count
	# -	- createBoard
	# -	- createCard
	# -	- createList
	# -	- createOrganization
	# -	- data
	# -	- date
	# -	- dateLastActivity
	# -	- dateLastView
	# -	- deactivated
	# -	- deleteAttachmentFromCard
	# -	- deleteBoardInvitation
	# -	- deleteCard
	# -	- deleteOrganizationInvitation
	# -	- desc
	# -	- descData
	# -	- disablePowerUp
	# -	- displayName
	# -	- due
	# -	- edgeColor
	# -	- email
	# -	- emailCard
	# -	- enablePowerUp
	# -	- fullName
	# -	- idAttachmentCover
	# -	- idBoard
	# -	- idBoards
	# -	- idCard
	# -	- idChecklists
	# -	- idLabels
	# -	- idList
	# -	- idMember
	# -	- idMemberCreator
	# -	- idMembers
	# -	- idMembersVoted
	# -	- idOrganization
	# -	- idPremOrgsAdmin
	# -	- idShort
	# -	- initials
	# -	- invitations
	# -	- invited
	# -	- isUpload
	# -	- labelNames
	# -	- labels
	# -	- list
	# -	- logoHash
	# -	- makeAdminOfBoard
	# -	- makeNormalMemberOfBoard
	# -	- makeNormalMemberOfOrganization
	# -	- makeObserverOfBoard
	# -	- manualCoverAttachment
	# -	- me
	# -	- memberJoinedTrello
	# -	- memberType
	# -	- memberships
	# -	- mimeType
	# -	- mine
	# -	- minimal
	# -	- moveCardFromBoard
	# -	- moveCardToBoard
	# -	- moveListFromBoard
	# -	- moveListToBoard
	# -	- name
	# -	- normal
	# -	- open
	# -	- owners
	# -	- pinned
	# -	- pos
	# -	- powerUps
	# -	- prefs
	# -	- premiumFeatures
	# -	- previews
	# -	- products
	# -	- removeChecklistFromCard
	# -	- removeFromOrganizationBoard
	# -	- removeMemberFromCard
	# -	- shortLink
	# -	- shortUrl
	# -	- starred
	# -	- status
	# -	- subscribed
	# -	- type
	# -	- unconfirmedBoardInvitation
	# -	- unconfirmedOrganizationInvitation
	# -	- updateBoard
	# -	- updateCard
	# -	- updateCard:closed
	# -	- updateCard:desc
	# -	- updateCard:idList
	# -	- updateCard:name
	# -	- updateCheckItemStateOnCard
	# -	- updateChecklist
	# -	- updateList
	# -	- updateList:closed
	# -	- updateList:name
	# -	- updateMember
	# -	- updateOrganization
	# -	- url
	# -	- username
	# -	- uses
	# -	- visible
	# -	- website
	#
	# Default: ["default"]
	config(:fields, :validate => :array, :default => ["default"])

	# A hash of arrays which is used to cull entities.
	# This is the master hash:
	# 	{
	# 	"cards" => 			["open", "closed", "visible"],
	# 	"lists" => 			["open", "closed"],
	# 	"members" => 		["admins", "normal", "owners"],
	# 	"membersInvited" => ["admins", "normal", "owners"],
	# 	"boards" =>  		["closed", "members", "open", "organization", 
	# 						 "pinned", "public", "starred", "unpinned"]
	# 	}
	#
	# Ommiting a key from the hash will prevent that entity from being culled.
	# Adding a filter to an entity's array will cause it to be culled by that.
	# filter.  For instance, if cards was set to ["open"], then trello would 
	# only return open cards.
	#
	# Default: {}, which means nothing will be culled.
	config(:filters, :validate => :hash, :default => {})

	# Do not change this.
	# Defualt: "json_lines"
	default(:codec, "json_lines")

	# The port trello listens on for REST requests.
	# Default: 443
	config(:port, :validate => :number, :default => 443)

	# An array of organizations from which to derive board ids.
	# This is not used if board ids are provided.
	config(:organizations, :validate => :array, :required => true)

	# Coerce output field names into snake_case.
	# Default: false
	config(:snake_case, :validate => :boolean, :default => false)

	# The frewuncy with wich to query Trello, in seconds.
	# Default: 3600 (1 hour)
	config(:interval, :validate => :number, :default => 3600)

	# Trello oauth key
	config(:key, :validate => :string, :required => true)

	# Trello oauth secret
	config(:token, :validate => :string, :required => true)

	# An array of ids of boards to be parsed.
	# A board id (shortLink actually) can be found in its URL.
	# For instance, in this URL, https://trello.com/b/dFsjpzeN/logstash, the
	# board id is dFsjpzeN.
	# 
	# If no ids are given, this plugin will query Trello based upon all the
	# boards assosciated with your organization.
	# 
	# Default: [] 
	config(:board_ids, :validate => :array, :default => [])

	# An array of event types to be emitted.
	# Output types include:
	# -	board
	# -	memberships
	# -	labels
	# -	cards
	# -	lists
	# -	members
	# -	checklists
	# -	action
	# If output_types is set to ["all"], then all types will be emitted.
	# 
	# Default: ["all"]
	config(:output_types, :validate => :array, :default => ["all"])

	# An array of fields to be excluded from all events emitted.
	# Default: []
	config(:exclude_fields, :validate => :array, :default => [])
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
		@host = Socket.gethostname
		@client = TrelloUtils::TrelloClient.new({
				organizations: @organizations,
				key:           @key,
				token:         @token,
				board_ids:     @board_ids,
				fields:        @fields,
				filters:       @filters,
				port:          @port
		})
	end
	# --------------------------------------------------------------------------

	private
	def recursive_has_key?(data, keys)
		if not data.is_a?(Hash)
			return false
		end    
		if data.has_key?(keys[0])
			if keys.length > 1
				recursive_has_key?(data[keys[0]], keys[1..-1])
			else
				return true
			end
		else
			return false
		end
	end

	private
	def fieldref_to_array(fieldref)
		output = fieldref.split("][")
		output.map! { |item| item.gsub(/\[|\]/, "") }
		return output
	end
	
	private
	def exclude_fields!(event)
		@exclude_fields.each_with_index do |field|
			f = fieldref_to_array(field)
			if recursive_has_key?(event.to_hash, f)
				event.remove(field)
			end
		end
	end
	
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
	def flatten(data)
		func = lambda do |key, val|
			output = val
			ds_type = get_data_structure_type(val)
			if ds_type == "array_of_hashes"
				return group(output)
			else
				return output
			end
		end
		return recurse(data.clone, func, func)
	end

	# private
	# def clean_lut(lut)
	# 	def _remove_entities(data)
	# 		if not data.is_a?(Hash)
	# 			return data # leaf (stop recursion here)
	# 		end

	# 		store = {}
	# 		data.each do |key, val|
	# 			if not @@all_entities_and_ids.include?(key)
	# 				if val.is_a?(Hash)
	# 					store[key] = _remove_entities(val)
	# 				else
	# 					store[key] = val
	# 				end
	# 			end
	# 		end
	# 		return store
	# 	end

	# 	lut = lut.clone
	# 	lut.to_a.each do|ent_type, ids|
	# 		ids.to_a.each do |item|
	# 			temp = flatten(item.clone)
	# 			lut[ent_type][item] = _remove_entities(temp)
	# 		end
	# 	end
	# 	return lut
	# end

	private
	def recurse(data, nonhash_func=nil, hash_func=nil, key_func=nil)
		hash_func = lambda { |key, val| val } if hash_func.nil?
		nonhash_func = lambda { |key, val| val } if nonhash_func.nil?
		key_func = lambda { |key| key } if key_func.nil?
		
		if not data.is_a?(Hash)
			return data # leaf (stop recursion here)
		end

		store = {}
		data.each do |key, val|
			if val.is_a?(Hash)
				store[key_func.call(key)] = recurse(hash_func.call(key, val), 
													nonhash_func, hash_func, key_func)
			else
				store[key_func.call(key)] = nonhash_func.call(key, val)
			end
		end
		return store
	end

	private
	def conform_field_names(data, form=nil)
		if not data.is_a?(Hash)
			return data
		end
		key_transformer = lambda do |key|
			if @@all_ids.include?(key)
				# clobber non-id fields with id fields
				new_key = key.gsub(/^id/, '')
				new_key = new_key[0].downcase + new_key[1..-1]
				if not form.nil?
					if form == 'plural'
						if not /ed$/.match(new_key)
							new_key = new_key.pluralize
						end
					elsif form == 'singular'
						new_key = new_key.singularize
					end
				end
				return new_key
			else
				return key
			end
		end
		return recurse(data.clone, nil, nil, key_transformer)
	end

	private
	def expand_entities(data, lut)
		func = lambda do |key, val|
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
						new_item = flatten(new_item)
						output.push(new_item)
					end

				elsif ds_type == "array_of_strings"
					val.each do |id|
						if l.has_key?(id)
							output.push(l[id])
							# output.push( flatten(l[id]) )
						end
					end
				end
				output = group(output)

			elsif ds_type == "empty_array"
				output = nil

			elsif ds_type == "hash_with_id"
				if l.has_key?(val["id"])
					output = l[val["id"]]
					# output = flatten(output)
				end

			elsif ds_type == "String"
				if l.has_key?(val)
					output = l[val]
					# output = flatten(output)
				end
			end

			return conform_field_names(output)
		end
		return recurse(data.clone, func, func)
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
		# lut = clean_lut(lut)

		@output_types.each do |out_type|
			if response.has_key?(out_type)
				response[out_type].each do |source|
					out_type_ = out_type.singularize
					data = clean_data(source)
					data = conform_field_names(data, 'plural')
					data = expand_entities(data, lut)
					all_ent = @@all_entities.clone
					all_ent.delete("entities")
					data = collapse(data, out_type_, all_ent)
					data = flatten(data)

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
					if out_type_ == "action"
						_timestamp = data["action"]["date"]
						data["action"].delete("date")
					end
					event = LogStash::Event.new(
						"host" => @host, 
						"type" => @type + '_' + out_type_,
						"@timestamp" => _timestamp,
						"message" => JSON.dump(source) )
					data.each do |key, val|
						event[key] = val
					end
					exclude_fields!(event)
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