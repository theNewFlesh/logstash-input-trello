# encoding: utf-8

require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname
require 'addressable/uri'
require 'net/http'
require 'json'
require 'time'
require 'set'
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
		"check_items"
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
		"powerup"
	]

	@@all_entities = @@singular_entities + @@plural_entities

	@@plural_ids = [
		"idChecklists",
		"idLabels",
		"idMembers"
	]

	@@singular_ids = [
		"idBoard",
		"idList"
	]

	@@all_ids = @@singular_ids + @@plural_ids

	default(:codec, "json_lines")
		# defualt: json_lines

	config(:port, :validate => :number, :default => 443)
		# The port trello listens on for REST requests

	config(:organizations, :validate => :array, :required => true)
		# an array of organizations from which to derive board ids

	config(:entities, :validate => :array, :required => true, :default => ["all"])
		# valid values:
		# 	all
		#	membersInvited
		#	labels
		#	lists
		#	memberships
		#	actions
		#	members
		#	checklists
		#	cards
		##boards
		##organizations

	config(:snake_case, :validate => :boolean, :default => false)
		# coerce output field names into snake_case

	config(:interval, :validate => :number, :default => 1800)
		# query interval
		# default: 30 minutes

	config(:key, :validate => :string, :required => true)
		# oauth key

	config(:token, :validate => :string, :required => true)
		# oauth secret
	# --------------------------------------------------------------------------
	
	config(:actions, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	addAttachmentToCard
		# 	addChecklistToCard
		# 	addMemberToBoard
		# 	addMemberToCard
		# 	addMemberToOrganization
		# 	addToOrganizationBoard
		# 	commentCard
		# 	convertToCardFromCheckItem
		# 	copyBoard
		# 	copyCard
		# 	copyCommentCard
		# 	createBoard
		# 	createCard
		# 	createList
		# 	createOrganization
		# 	deleteAttachmentFromCard
		# 	deleteBoardInvitation
		# 	deleteCard
		# 	deleteOrganizationInvitation
		# 	disablePowerUp
		# 	emailCard
		# 	enablePowerUp
		# 	makeAdminOfBoard
		# 	makeNormalMemberOfBoard
		# 	makeNormalMemberOfOrganization
		# 	makeObserverOfBoard
		# 	memberJoinedTrello
		# 	moveCardFromBoard
		# 	moveCardToBoard
		# 	moveListFromBoard
		# 	moveListToBoard
		# 	removeChecklistFromCard
		# 	removeFromOrganizationBoard
		# 	removeMemberFromCard
		# 	unconfirmedBoardInvitation
		# 	unconfirmedOrganizationInvitation
		# 	updateBoard
		# 	updateCard
		# 	updateCard:closed
		# 	updateCard:desc
		# 	updateCard:idList
		# 	updateCard:name
		# 	updateCheckItemStateOnCard
		# 	updateChecklist
		# 	updateList
		# 	updateList:closed
		# 	updateList:name
		# 	updateMember
		# 	updateOrganization

	config(:actions_entities, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false

	config(:actions_since, :validate => :string, :default => "last query")
		# valid values:
		# 	last query
		# 	a date
		# 	null
		# 	lastView

	config(:actions_limit, :validate => :number, :default => 1000)
		# valid values:
		# 	an integer from 0 to 1000

	config(:action_fields, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	data
		# 	date
		# 	idMemberCreator
		# 	type

	config(:action_member, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false

	config(:action_member_fields, :validate => :array, 
		   :default => ["all"]) 
				# ["avatarHash",
				#  "fullName",
				#  "initials",
				#  "username"])
		# valid values:
		# 	all
		# 	avatarHash
		# 	bio
		# 	bioData
		# 	confirmed
		# 	fullName
		# 	idPremOrgsAdmin
		# 	initials
		# 	memberType
		# 	products
		# 	status
		# 	url
		# 	username

	config(:action_member_creator, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false

	config(:action_member_creator_fields, :validate => :array, 
		   :default => ["all"])
				# ["avatarHash",
				#  "fullName",
				#  "initials",
				#  "username"])
		# valid values:
		# 	all
		# 	avatarHash
		# 	bio
		# 	bioData
		# 	confirmed
		# 	fullName
		# 	idPremOrgsAdmin
		# 	initials
		# 	memberType
		# 	products
		# 	status
		# 	url
		# 	username
	# --------------------------------------------------------------------------
	
	config(:board_ids, :validate => :array, :default => [])
		# ids of boards to be queried

	config(:board_filter, :validate => :array,
		   :required => true, :default => ["all"])
		# valid values:
		# 	all
		# 	closed
		# 	members
		# 	open
		# 	organization
		# 	pinned
		# 	public
		# 	starred
		# 	unpinned

	# board fields
	config(:board_fields, :validate => :array,
		   :default => [
		   	"closed",
			"dateLastActivity",
			"dateLastView",
			"desc",
			"descData",
			"invitations",
			"invited",
			"labelNames",
			"name",
			"pinned",
			"prefs",
			"shortLink",
			"shortUrl",
			"starred",
			"subscribed",
			"url"])
			# ["name",
			# "desc",
			# "descData",
			# "closed",
			# "idOrganization",
			# "pinned",
			# "url",
			# "shortUrl",
			# "prefs",
			# "labelNames"])
		# valid values:
		# 	all
		# 	closed
		# 	dateLastActivity
		# 	dateLastView
		# 	desc
		# 	descData
		# 	idOrganization
		# 	invitations
		# 	invited
		# 	labelNames
		# 	memberships
		# 	name
		# 	pinned
		# 	powerUps
		# 	prefs
		# 	shortLink
		# 	shortUrl
		# 	starred
		# 	subscribed
		# 	url

	config(:board_stars, :validate => :string, :default => "mine")
		# valid values:
		# 	mine
		# 	none
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:cards, :validate => :string, :default => "all")
		# valid values:
		# 	all
		# 	closed
		# 	none
		# 	open
		# 	visible

	config(:card_fields, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	badges
		# 	checkItemStates
		# 	closed
		# 	dateLastActivity
		# 	desc
		# 	descData
		# 	due
		# 	email
		# 	idAttachmentCover
		# 	idBoard
		# 	idChecklists
		# 	idLabels
		# 	idList
		# 	idMembers
		# 	idMembersVoted
		# 	idShort
		# 	labels
		# 	manualCoverAttachment
		# 	name
		# 	pos
		# 	shortLink
		# 	shortUrl
		# 	subscribed
		# 	url

	config(:card_attachments, :validate => :boolean, :default => true)
		# valid values:
		# 	A boolean value or &quot;cover&quot; for only card cover attachments

	config(:card_attachment_fields, :validate => :array, 
			:default => ["all"])
		# valid values:
		# 	all
		# 	bytes
		# 	date
		# 	edgeColor
		# 	idMember
		# 	isUpload
		# 	mimeType
		# 	name
		# 	previews
		# 	url

	config(:card_checklists, :validate => :string, :default => "all")
		# valid values:
		# 	all
		# 	none

	config(:card_stickers, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:labels, :validate => :string, :default => "all")
		# valid values:
		# 	all
		# 	none

	config(:label_fields, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	color
		# 	idBoard
		# 	name
		# 	uses

	config(:labels_limit, :validate => :number, :default => 1000)
		# valid values:
		# 	a number from 0 to 1000
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:lists, :validate => :string, :default => "all")
		# valid values:
		# 	all
		# 	closed
		# 	none
		# 	open

	config(:list_fields, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	closed
		# 	idBoard
		# 	name
		# 	pos
		# 	subscribed
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:memberships, :validate => :array, :default => ["all"])
		# valid values:
		# 	all
		# 	active
		# 	admin
		# 	deactivated
		# 	me
		# 	normal

	config(:memberships_member, :validate => :boolean, :default => false)
		# valid values:
		# 	true
		# 	false

	config(:memberships_member_fields, :validate => :array,
			:default => ["all"])
		   # :default => ["fullName", "username"])
		# valid values:
		# 	all
		# 	avatarHash
		# 	bio
		# 	bioData
		# 	confirmed
		# 	fullName
		# 	idPremOrgsAdmin
		# 	initials
		# 	memberType
		# 	products
		# 	status
		# 	url
		# 	username

	# CONFIG WITH ENTITIES
	config(:members, :validate => :string, :default => "all")
		# valid values:
		# 	admins
		# 	all
		# 	none
		# 	normal
		# 	owners

	config(:member_fields, :validate => :array,
		   :default => [
				"avatarHash",
				"bio",
				"confirmed",
				"fullName",
				"initials",
				"memberType",
				"status",
				"url",
				"username"
			])
				# ["avatarHash",
				#  "initials",
				#  "fullName",
				#  "username",
				#  "confirmed"])
		# valid values:
		# 	all
		# 	avatarHash
		# 	bio
		# 	bioData
		# 	confirmed
		# 	fullName
		# 	idPremOrgsAdmin
		# 	initials
		# 	memberType
		# 	products
		# 	status
		# 	url
		# 	username

	# CONFIG WITH ENTITIES
	config(:members_invited, :validate => :string, :default => "all")
		# valid values:
		# 	admins
		# 	all
		# 	none
		# 	normal
		# 	owners

	config(:members_invited_fields, :validate => :array,
		   :default => ["all"])
				# ["avatarHash",
				#  "initials",
				#  "fullName",
				#  "username"])
		# valid values:
		# 	all
		# 	avatarHash
		# 	bio
		# 	bioData
		# 	confirmed
		# 	fullName
		# 	idPremOrgsAdmin
		# 	initials
		# 	memberType
		# 	products
		# 	status
		# 	url
		# 	username
	# --------------------------------------------------------------------------
	
	# CONFIG WITH ENTITIES
	config(:checklists, :validate => :string, :default => "all")
		# valid values:
		# 	all
		# 	none

	config(:checklist_fields, :validate => :array, :default => ["all"])
		# valid values: all or a comma-separated list of:
		# 	all
		# 	idBoard
		# 	idCard
		# 	name
		# 	pos
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:organization, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false

	config(:organization_fields, :validate => :array,
		   :default => [
				"billableMemberCount",
				"desc",
				"descData",
				"displayName",
				"invitations",
				"invited",
				"logoHash",
				"name",
				"prefs",
				"premiumFeatures",
				"products",
				"url",
				"website"
			])
		   # :default => ["name", "displayName"])
		# valid values:
		# 	all
		# 	billableMemberCount
		# 	desc
		# 	descData
		# 	displayName
		# 	idBoards
		# 	invitations
		# 	invited
		# 	logoHash
		# 	memberships
		# 	name
		# 	powerUps
		# 	prefs
		# 	premiumFeatures
		# 	products
		# 	url
		# 	website

	config(:organization_memberships, :validate => :array,
		   :default => ["none"])
		# valid values:
		# 	all
		# 	active
		# 	admin
		# 	deactivated
		# 	me
		# 	normal
	# --------------------------------------------------------------------------

	# CONFIG WITH ENTITIES
	config(:my_prefs, :validate => :boolean, :default => true)
		# valid values:
		# 	true
		# 	false
	# --------------------------------------------------------------------------

	public
	def register()
		def array_to_uri(item)
			if item.to_a.empty?
				return ""
			else
				return item.join(",")
			end
		end

		if @entities == ["all"]
			@entities =	[
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
				"powerups"
			]
		end

		@host = Socket.gethostname
		@board_filter                 = array_to_uri(@board_filter)

		@actions                      = array_to_uri(@actions)
		@actions_entities             = @actions_entities.to_s()
		# @actions_since                = @actions_since
		@actions_limit                = @actions_limit.to_s()
		@action_fields                = array_to_uri(@action_fields)
		@action_member                = @action_member.to_s()
		@action_member_fields         = array_to_uri(@action_member_fields)
		@action_member_creator        = @action_member_creator.to_s()
		@action_member_creator_fields = array_to_uri(@action_member_creator_fields)
		@board_fields                 = array_to_uri(@board_fields)
		# @board_stars                  = @board_stars
		# @cards                        = @cards
		@card_fields                  = array_to_uri(@card_fields)
		@card_attachments             = @card_attachments.to_s()
		@card_attachment_fields       = array_to_uri(@card_attachment_fields)
		@card_checklists              = @card_checklists
		@card_stickers                = @card_stickers.to_s()
		# @labels                       = @labels
		@label_fields                 = array_to_uri(@label_fields)
		@labels_limit                 = @labels_limit.to_s()
		# @lists                        = @lists
		@list_fields                  = array_to_uri(@list_fields)
		@memberships                  = array_to_uri(@memberships)
		@memberships_member           = @memberships_member.to_s()
		@memberships_member_fields    = array_to_uri(@memberships_member_fields)
		# @members                      = @members
		@member_fields                = array_to_uri(@member_fields)
		# @members_invited              = @members_invited
		@members_invited_fields       = array_to_uri(@members_invited_fields)
		# @checklists                   = @checklists
		@checklist_fields             = array_to_uri(@checklist_fields)
		@organization                 = @organization.to_s()
		@organization_fields          = array_to_uri(@organization_fields)
		@organization_memberships     = array_to_uri(@organization_memberships)
		@my_prefs                     = @my_prefs.to_s()
	end

	private
	def _board_ids()
		# get board ids
		if !@board_ids.empty?
			return @board_ids
		else
			board_ids = Set.new()
			@organizations.each do |org|
				uri =  "/1/organizations/"
				uri += org
				uri += "/boards/"
				uri += @board_filter + "?"
				uri += "&key="       + @key
				uri += "&token="     + @token
				response = issue_request(uri)
				response.each do |item|
					board_ids.add(item["shortLink"])
				end
			end
			return board_ids
		end
	end

	private
	def get_uri(board_id, query_time)
		actions_since = @actions_since
		if @actions_since == "last query"
			actions_since = query_time
		end
		# construct uri
		uri =  "/1/boards/"
		uri += board_id + '?'
		uri += "actions="                      + @actions
		uri += "&actions_entities="            + @actions_entities
		uri += "&actions_format="              + "list"
		uri += "&actions_since="               + actions_since
		uri += "&actions_limit="               + @actions_limit
		uri += "&action_fields="               + @action_fields
		uri += "&action_member="               + @action_member
		uri += "&action_member_fields="        + @action_member_fields
		uri += "&action_memberCreator="        + @action_member_creator
		uri += "&action_memberCreator_fields=" + @action_member_creator_fields
		uri += "&fields="                      + @board_fields
		uri += "&board_stars="                 + @board_stars		
		uri += "&cards="                       + @cards
		uri += "&card_fields="                 + @card_fields
		uri += "&card_attachments="            + @card_attachments
		uri += "&card_attachment_fields="      + @card_attachment_fields
		uri += "&card_checklists="             + @card_checklists
		uri += "&card_stickers="               + @card_stickers
		uri += "&labels="                      + @labels
		uri += "&label_fields="                + @label_fields
		uri += "&labels_limit="                + @labels_limit
		uri += "&lists="                       + @lists
		uri += "&list_fields="                 + @list_fields
		uri += "&memberships="                 + @memberships
		uri += "&memberships_member="          + @memberships_member
		uri += "&memberships_member_fields="   + @memberships_member_fields
		uri += "&members="                     + @members
		uri += "&member_fields="               + @member_fields
		uri += "&membersInvited="              + @members_invited
		uri += "&membersInvited_fields="       + @members_invited_fields
		uri += "&checklists="                  + @checklists
		uri += "&checklist_fields="            + @checklist_fields
		uri += "&organization="                + @organization
		uri += "&organization_fields="         + @organization_fields
		uri += "&organization_memberships="    + @organization_memberships
		uri += "&myPrefs="                     + @my_prefs
		uri += "&key="                         + @key
		uri += "&token="                       + @token
		return uri
	end

	private
	def issue_request(uri)
		response = Net::HTTP.new("api.trello.com", @port)
		response.use_ssl = true
		response = response.request_get(uri, {"Connection" => "close"})
		code = response.code.to_i()
		if 199 < code and code < 300
			return JSON.load(response.body)
		else
			@logger.warn("HTTP request error", + response)
			raise StandardError.new()
		end
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

	null_func = lambda { |store, key, val| return val }

	public
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
	def clean_data(data, lut)
		data = data.clone
		# remove actions data field
		if data.has_key?("data")
			data["data"].each do |key, val|
				data[key] = val
			end
			data.delete("data")
		end
		if data.has_key?("board")
			data.delete("board")
		end

		data.to_a.each do |key, val|
			if @@all_ids.include?(key)
				# clobber non-id fields with id fields
				new_key = key.gsub(/^id/, '')
				new_key = new_key[0].downcase + new_key[1..-1]
				new_key = pluralize(new_key)
				data[new_key] = val
				data.delete(key)
			end
		end
		return data
	end
	
	public
	def expand_entities(data, lut)
		func = lambda do |store, key, val|
			if @@plural_entities.include?(key)
				if val.is_a?(Array)
					if not val.empty?
						if val[0].is_a?(String)
							output = []
							val.each do |id|
								if lut[key].has_key?(id)
									output.push(lut[key][id])
								end
							end
							if not output.empty?
								return reduce(output)
							end
						elsif val[0].is_a?(Hash)
							return reduce(val)
						end
					end
				else
					return val
				end
			elsif @@singular_entities.include?(key) and val.has_key?("id")
				l = lut[pluralize(key)]
				if l.has_key?(val["id"])
					return l[ val["id"] ]
				end			
			else
				return val
			end
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
	def depluralize(item)
		return item.gsub(/s$/, "")
	end

	private
	def pluralize(item)
		output = item.gsub(/s$/, "")
		return output + "s"
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
	def reduce(data)
		def _reduce(data)
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
					return _reduce(data)
				end
			end
		else
			# return data if data structure is wrong
			return data
		end
	end

	private
	def to_snake_case(data)
		def _to_snake_case(item)
			output = item.gsub(/::/, '/')\
				.gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2')\
				.gsub(/([a-z\d])([A-Z])/,'\1_\2')\
				.tr("-", "_")\
				.downcase
			return output
		end

		data.select { |index, item| index.map! { |item| _to_snake_case(item) } }
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

		@entities.each do |ent_type|
			if response.has_key?(ent_type)
				response[ent_type].each do |source|
					singular = ent_type[0..-2]
					# puts JSON.dump(source), "\n"
					data = clean_data(source, lut)
					# puts JSON.dump(data), "\n"
					data = expand_entities(data, lut)
					# puts JSON.dump(data), "\n"
					all_ent = @@all_entities
					all_ent.delete("entities")
					data = collapse(data, singular, all_ent)
					# puts JSON.dump(data), "\n"
					
					# shuffle board info into data
					if ["members"].include?(ent_type)
						data["board"] = response["board"]
					end
					
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
		query_time = Time.now - @interval
		query_time = query_time.strftime('%Y-%m-%dT%H:%M:%S%z')
		Stud.interval(@interval) do
			_board_ids.each do |board_id|
				uri = get_uri(board_id, query_time)
				response = nil
				begin
					response = issue_request(uri)
				rescue StandardError
					next
				end
				process_response(response, queue)
			end
			query_time = Time.now.strftime('%Y-%m-%dT%H:%M:%S%z')
		end
	end
end
