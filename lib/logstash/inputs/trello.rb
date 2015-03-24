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

# The trello filter is used querying the trello database and returning the resulting
# cards as events

class LogStash::Inputs::Trello < LogStash::Inputs::Base
	config_name "trello"
	milestone 1

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

	config(:actions_entities, :validate => :boolean, :default => false)
		# valid values:
		# 	true
		# 	false

	config(:actions_since, :validate => :string, :default => "last query")
		# valid values:
		# 	last query
		# 	a date
		# 	null
		# 	lastView

	config(:actions_limit, :validate => :number, :default => 50)
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
		   :default => 
				["avatarHash",
				 "fullName",
				 "initials",
				 "username"])
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
		   :default =>
				["avatarHash",
				 "fullName",
				 "initials",
				 "username"])
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
		   :default => 
				["name",
				"desc",
				"descData",
				"closed",
				"idOrganization",
				"pinned",
				"url",
				"shortUrl",
				"prefs",
				"labelNames"])
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

	config(:board_stars, :validate => :string, :default => "none")
		# valid values:
		# 	mine
		# 	none

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

	config(:card_attachments, :validate => :boolean, :default => false)
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

	config(:card_checklists, :validate => :string, :default => "none")
		# valid values:
		# 	all
		# 	none

	config(:card_stickers, :validate => :boolean, :default => false)
		# valid values:
		# 	true
		# 	false

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

	config(:labels_limit, :validate => :number, :default => 50)
		# valid values:
		# 	a number from 0 to 1000
	
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
		   :default => ["fullName", "username"])
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
	config(:members, :validate => :string, :default => "none")
		# valid values:
		# 	admins
		# 	all
		# 	none
		# 	normal
		# 	owners

	config(:member_fields, :validate => :array,
		   :default => 
				["avatarHash",
				 "initials",
				 "fullName",
				 "username",
				 "confirmed"])
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
	config(:members_invited, :validate => :string, :default => "none")
		# valid values:
		# 	admins
		# 	all
		# 	none
		# 	normal
		# 	owners

	config(:members_invited_fields, :validate => :array,
		   :default => 
				["avatarHash",
				 "initials",
				 "fullName",
				 "username"])
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

	# CONFIG WITH ENTITIES
	config(:organization, :validate => :boolean, :default => false)
		# valid values:
		# 	true
		# 	false

	config(:organization_fields, :validate => :array, 
		   :default => ["name", "displayName"])
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

	# CONFIG WITH ENTITIES
	config(:my_prefs, :validate => :boolean, :default => false)
		# valid values:
		# 	true
		# 	false

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
			@entities = ["cards", "lists", "labels", "actions"]
			# members_invited
			# memberships
			# members
			# checkists
		end

		@host = Socket.gethostname
		@board_filter                 = array_to_uri(@board_filter)

		@actions                      = array_to_uri(@actions)
		@actions_entities             = @actions_entities.to_s()
		@actions_since                = @actions_since
		@actions_limit                = @actions_limit.to_s()
		@action_fields                = array_to_uri(@action_fields)
		@action_member                = @action_member.to_s()
		@action_member_fields         = array_to_uri(@action_member_fields)
		@action_member_creator        = @action_member_creator.to_s()
		@action_member_creator_fields = array_to_uri(@action_member_creator_fields)
		@board_fields                 = array_to_uri(@board_fields)
		@board_stars                  = @board_stars
		@cards                        = @cards
		@card_fields                  = array_to_uri(@card_fields)
		@card_attachments             = @card_attachments.to_s()
		@card_attachment_fields       = array_to_uri(@card_attachment_fields)
		@card_checklists              = @card_checklists
		@card_stickers                = @card_stickers.to_s()
		@labels                       = @labels
		@label_fields                 = array_to_uri(@label_fields)
		@labels_limit                 = @labels_limit.to_s()
		@lists                        = @lists
		@list_fields                  = array_to_uri(@list_fields)
		@memberships                  = array_to_uri(@memberships)
		@memberships_member           = @memberships_member.to_s()
		@memberships_member_fields    = array_to_uri(@memberships_member_fields)
		@members                      = @members
		@member_fields                = array_to_uri(@member_fields)
		@members_invited              = @members_invited
		@members_invited_fields       = array_to_uri(@members_invited_fields)
		@checklists                   = @checklists
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
	def coerce_nulls(data)
		output = {}
		data.each do |key, val|
			if val == ""
				output[key] = nil
			else
				output[key] = val
			end
		end
		return output
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

		output = {}
		data.each do |key, val|
			if val.is_a?(Hash)
				output[_to_snake_case(key)] = to_snake_case(val)
			else
				output[_to_snake_case(key)] = val
			end
		end
		return output
	end

	private
	def reduce(data)
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

	private
	def reduce_data(data, entity)
		output = {}
		ent = entity[0..-2]
		output[ent] = {}
		data.each do |key, val|
			if val.is_a?(Array)
				if val.length > 0
					if val[0].is_a?(Hash)
						output[key] = reduce(val)
					end
				else 
					next
				end
			else
				if ["type", "id", "version", "name"].include?(key)
					output[ent][key] = val
				else
					output[key] = val
				end
			end
		end
		# remove data field from entities that have one (ie actions)
		if output.has_key?("data")
			output["data"].each do |key, val|
				output[key] = val
			end
			output.delete("data")
		end
		return output
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

	# private
	# def create_inverted_index(data, func)
	# 	output = {}
	# 	data.each { |item| output[func.call(item)] = item }
	# 	return output
	# end

	# private
	# def substitute_entity(data, entity, index, func)
	# 	for item in data
	# 		id = func.call(item)
	# 		data[entity] = index[id]
	# 	end
	# 	return data
	# end

	# private
	# def conform_cards(data)
	# 	index = create_inverted_index()
	# 	substitute_entity(data, "list", )
	# end

	private
	def process_response(response, queue)
		# create board data
		board_fields = [ 'closed',
						 'dateLastActivity',
						 'dateLastView',
						 'desc',
						 'descData',
						 'id',
						 'idOrganization',
						 'invited',
						 'labelNames',
						 'name',
						 'pinned',
						 'prefs',
						 'shortLink',
						 'shortUrl',
						 'starred',
						 'subscribed',
						 'url']
		board = {}
		board_fields.each do |field|
			if response.has_key?(field)
				board[field] = response[field]
			end
		end

		@entities.each do |entity|
			if response[entity].length > 0
				response[entity].each do |data|
					data = coerce_nulls(data)
					data = reduce_data(data, entity)
					
					# incorporate board data
					if data.has_key?("board")
						board.each do |key, val|
							if not data["board"].has_key?(key)
								data["board"][key] = val
							end
						end
					else
						data["board"] = board
					end

					if @snake_case
						data = to_snake_case(data)
					end
					event = nil
					# set the timestamp of actions to their date field
					if entity == "actions"
						timestamp = data["action"]["date"]
						data["action"].delete("date")
						event = LogStash::Event.new(
							"host" => @host, 
							"type" => @type + '_' + entity[0..-2],
							"@timestamp" => timestamp)
					else
						event = LogStash::Event.new("host" => @host, 
							"type" => @type + '_' + entity[0..-2])
					end
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