# encoding: utf-8

require "socket"
require 'addressable/uri'
require 'net/http'
require 'json'
require 'set'

module TRELLO_QUERY
	class TrelloQuery
		def initialize( kwargs={
							"organizations" => [],
							"key"           => nil,
							"token"         => nil,
							"board_ids"     => [],
							"fields"        => PARAM_DEFAULT_FIELDS,
							"entities"      => PARAM_DEFAULT_ENTITIES,
							"filters"       => {},
							"port"          => 443
						})

			organizations = kwargs["organizations"]
			key           = kwargs["key"]
			token         = kwargs["token"]
			board_ids     = kwargs["board_ids"]
			fields        = kwargs["fields"]
			entities      = kwargs["entities"]
			filters       = kwargs["filters"]
			port          = kwargs["port"]

			@_host = Socket.gethostname
			@_port = port
				# The port trello listens on for REST requests
			@_organizations = organizations
				# an array of organizations from which to derive board ids
			@_key = key
				# oauth key
			@_token = token
				# oauth secret

			board_filter = "all"
			if not filters.empty?
				if filters.has_key?("boards")
					if not filters["boards"].empty?
						board_filter = array_to_uri( filters["boards"] )
					end
				end
			end
			
			if not board_ids.empty?
				@_board_ids = get_board_ids(board_ids, board_filter)
			else
				@board_ids = board_ids
			end

			# create fields hash
			new_fields = ALL_FIELDS.clone
			new_fields.to_a.each do |key, val|
				fields.each do |field|
					if not val.include?(field)
						new_fields[key].delete(field)
					end
	            end
	            new_fields[key] = array_to_uri(new_fields[key])
			end

			# create entities hash
			new_entities = {}
	        entities.each do |entity|
				state = ALL_ENTITIES[entity]
				if state == "false"
					new_entities[entity] = "true"
				elsif state == "none"
					if entity == "boardStars"
						new_entities[entity] = "mine"
					else
						new_entities[entity] = "all"
					end
				end
			end

			# mutate entities hash according to filters
			filters.each do |key, val|
				if new_entities.include?(key)
					if val != "none"
						new_entities[key] = array_to_uri(filters[entity])
					end
				end
			end

			@_filters    = filters
			@_fields     = new_fields
			@_entities   = new_entities
			@_parameters = new_fields.merge(new_entities)
		end
		# --------------------------------------------------------------------------

		private
		def array_to_uri(item)
			if item.empty?
				return ""
			else
				return item.join(",")
			end
		end

		private
		def get_board_ids(board_ids, board_filter)
			# get board ids
			if not board_ids.empty?
				return board_ids
			else
				board_ids = Set.new()
				@_organizations.each do |org|
					uri =  "/1/organizations/"
					uri += org
					uri += "/boards/"
					uri += board_filter + "?"
					uri += "&key="       + @_key
					uri += "&token="     + @_token
					response = issue_request(uri)
					response.each do |item|
						board_ids.add(item["shortLink"])
					end
				end
				return board_ids.to_a
			end
		end

		public
		def get_uri(board_id, actions_since)
			# construct uri
			uri =  "/1/boards/"       + board_id
			uri += "?actions_format=" + "list"
			uri += "&actions_since="  + actions_since
			uri += "&actions_limit="  + "1000"
			uri += "&labels_limit="   + "1000"
			uri += "&key="            + @_key
			uri += "&token="          + @_token
			temp = ""
			@_parameters.each { |key, val| temp += "&" + key + "=" + val }
			uri += temp
			return uri
		end

		public
		def issue_request(uri)
			response = Net::HTTP.new("api.trello.com", self.port)
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

		# accessors
		public
		def filters()
			return @_filters
		end

		public
		def fields()
			return @_fields
		end

		public
		def entities()
			return @_entities
		end

		public
		def parameters()
			return @_parameters
		end

		public
		def host()
			return @_host
		end

		public
		def port()
			return @_port
		end

		public
		def organizations()
			return @_organizations
		end

		public
		def key()
			return @_key
		end

		public
		def token()
			return @_token
		end

		public
		def board_ids()
			return @_board_ids
		end
	end
	# ------------------------------------------------------------------------------


	ALL_FIELDS = {
	    "actions" => [
			"addAttachmentToCard",
			"addChecklistToCard",
			"addMemberToBoard",
			"addMemberToCard",
			"addMemberToOrganization",
			"addToOrganizationBoard",
			"commentCard",
			"convertToCardFromCheckItem",
			"copyBoard",
			"copyCard",
			"copyCommentCard",
			"createBoard",
			"createCard",
			"createList",
			"createOrganization",
			"deleteAttachmentFromCard",
			"deleteBoardInvitation",
			"deleteCard",
			"deleteOrganizationInvitation",
			"disablePowerUp",
			"emailCard",
			"enablePowerUp",
			"makeAdminOfBoard",
			"makeNormalMemberOfBoard",
			"makeNormalMemberOfOrganization",
			"makeObserverOfBoard",
			"memberJoinedTrello",
			"moveCardFromBoard",
			"moveCardToBoard",
			"moveListFromBoard",
			"moveListToBoard",
			"removeChecklistFromCard",
			"removeFromOrganizationBoard",
			"removeMemberFromCard",
			"unconfirmedBoardInvitation",
			"unconfirmedOrganizationInvitation",
			"updateBoard",
			"updateCard",
			"updateCard:closed",
			"updateCard:desc",
			"updateCard:idList",
			"updateCard:name",
			"updateCheckItemStateOnCard",
			"updateChecklist",
			"updateList",
			"updateList:closed",
			"updateList:name",
			"updateMember",
			"updateOrganization"
		],

		"action_fields" => [
			"data",
			"date",
			"idMemberCreator",
			"type"
		],

		"action_member_fields" => [
			"avatarHash",
			"bio",
			"bioData",
			"confirmed",
			"fullName",
			"idPremOrgsAdmin",
			"initials",
			"memberType",
			"products",
			"status",
			"url",
			"username"
		],

		"action_memberCreator_fields" => [
			"avatarHash",
			"bio",
			"bioData",
			"confirmed",
			"fullName",
			"idPremOrgsAdmin",
			"initials",
			"memberType",
			"products",
			"status",
			"url",
			"username"
		],

		"card_fields" => [
			"badges",
			"checkItemStates",
			"closed",
			"dateLastActivity",
			"desc",
			"descData",
			"due",
			"email",
			"idAttachmentCover",
			"idBoard",
			"idChecklists",
			"idLabels",
			"idList",
			"idMembers",
			"idMembersVoted",
			"idShort",
			"labels",
			"manualCoverAttachment",
			"name",
			"pos",
			"shortLink",
			"shortUrl",
			"subscribed",
			"url"
		],

		"card_attachment_fields" => [
			"bytes",
			"date",
			"edgeColor",
			"idMember",
			"isUpload",
			"mimeType",
			"name",
			"previews",
			"url"
		],

		"label_fields" => [
			"color",
			"idBoard",
			"name",
			"uses"
		],

		"list_fields" => [
			"closed",
			"idBoard",
			"name",
			"pos",
			"subscribed"
		],

		"memberships" => [
			"active",
			"admin",
			"deactivated",
			"me",
			"normal"
		],

		"memberships_member_fields" => [
			"avatarHash",
			"bio",
			"bioData",
			"confirmed",
			"fullName",
			"idPremOrgsAdmin",
			"initials",
			"memberType",
			"products",
			"status",
			"url",
			"username"
		],

		"member_fields" => [
			"avatarHash",
			"bio",
			"bioData",
			"confirmed",
			"fullName",
			"idPremOrgsAdmin",
			"initials",
			"memberType",
			"products",
			"status",
			"url",
			"username"
		],

		"membersInvited_fields" => [
			"avatarHash",
			"bio",
			"bioData",
			"confirmed",
			"fullName",
			"idPremOrgsAdmin",
			"initials",
			"memberType",
			"products",
			"status",
			"url",
			"username"
		],

		"checklist_fields" => [
			"idBoard",
			"idCard",
			"name",
			"pos"
		],

		"organization_fields" => [
			"billableMemberCount",
			"desc",
			"descData",
			"displayName",
			"idBoards",
			"invitations",
			"invited",
			"logoHash",
			"memberships",
			"name",
			"powerUps",
			"prefs",
			"premiumFeatures",
			"products",
			"url",
			"website"
		],

		"organization_memberships" => [
			"active",
			"admin",
			"deactivated",
			"me",
			"normal"
		],

		"board_fields" => [
			"closed",
			"dateLastActivity",
			"dateLastView",
			"desc",
			"descData",
			"idOrganization",
			"invitations",
			"invited",
			"labelNames",
			"memberships",
			"name",
			"pinned",
			"powerUps",
			"prefs",
			"shortLink",
			"shortUrl",
			"starred",
			"subscribed",
			"url"
		]
	}

	ALL_ENTITIES = {
		"actions_entities"     => "false",
		"action_member"        => "false",
		"action_memberCreator" => "false",
		"card_stickers"        => "false",
		"memberships_member"   => "false",
		"organization"         => "false",
		"myPrefs"              => "false",
		"card_attachments"     => "false",
		"cards"                => "none",
		"card_checklists"      => "none",
		"boardStars"           => "none",
		"labels"               => "none",
		"lists"                => "none",
		"members"              => "none",
		"membersInvited"       => "none",
		"checklists"           => "none"
	}

	ALL_FILTERS = {
		"cards" => [
			"open",
			"closed",
			"visible"
		],

		"lists" => [
			"open",
			"closed"

		],

		"members" => [
			"admins",
			"normal",
			"owners"
		],

		"membersInvited" => [
			"admins",
			"normal",
			"owners"
		],

		"boards" => [
			"closed",
			"members",
			"open",
			"organization",
			"pinned",
			"public",
			"starred",
			"unpinned"
		]
	}

	PARAM_ALL_ENTITIES = [
		"actions_entities",
		"action_member",
		"action_memberCreator",
		"card_stickers",
		"memberships_member",
		"organization",
		"myPrefs",
		"card_attachments",
		"cards",
		"card_checklists",
		"boardStars",
		"labels",
		"lists",
		"members",
		"membersInvited",
		"checklists"
	]

	PARAM_DEFAULT_ENTITIES = [
		"actions_entities",
		"action_member",
		"action_memberCreator",
		"card_stickers",
		"memberships_member",
		"organization",
		"myPrefs",
		"card_attachments",
		"cards",
		"card_checklists",
		"boardStars",
		"labels",
		"lists",
		"members",
		"membersInvited",
		"checklists"
	]

	PARAM_ALL_FIELDS = [
		"active",
		"addAttachmentToCard",
		"addChecklistToCard",
		"addMemberToBoard",
		"addMemberToCard",
		"addMemberToOrganization",
		"addToOrganizationBoard",
		"admin",
		"admins",
		"avatarHash",
		"badges",
		"billableMemberCount",
		"bio",
		"bioData",
		"bytes",
		"checkItemStates",
		"closed",
		"color",
		"commentCard",
		"confirmed",
		"convertToCardFromCheckItem",
		"copyBoard",
		"copyCard",
		"copyCommentCard",
		"count",
		"createBoard",
		"createCard",
		"createList",
		"createOrganization",
		"data",
		"date",
		"dateLastActivity",
		"dateLastView",
		"deactivated",
		"deleteAttachmentFromCard",
		"deleteBoardInvitation",
		"deleteCard",
		"deleteOrganizationInvitation",
		"desc",
		"descData",
		"disablePowerUp",
		"displayName",
		"due",
		"edgeColor",
		"email",
		"emailCard",
		"enablePowerUp",
		"fullName",
		"idAttachmentCover",
		"idBoard",
		"idBoards",
		"idCard",
		"idChecklists",
		"idLabels",
		"idList",
		"idMember",
		"idMemberCreator",
		"idMembers",
		"idMembersVoted",
		"idOrganization",
		"idPremOrgsAdmin",
		"idShort",
		"initials",
		"invitations",
		"invited",
		"isUpload",
		"labelNames",
		"labels",
		"list",
		"logoHash",
		"makeAdminOfBoard",
		"makeNormalMemberOfBoard",
		"makeNormalMemberOfOrganization",
		"makeObserverOfBoard",
		"manualCoverAttachment",
		"me",
		"memberJoinedTrello",
		"memberType",
		"memberships",
		"mimeType",
		"mine",
		"minimal",
		"moveCardFromBoard",
		"moveCardToBoard",
		"moveListFromBoard",
		"moveListToBoard",
		"name",
		"normal",
		"open",
		"owners",
		"pinned",
		"pos",
		"powerUps",
		"prefs",
		"premiumFeatures",
		"previews",
		"products",
		"removeChecklistFromCard",
		"removeFromOrganizationBoard",
		"removeMemberFromCard",
		"shortLink",
		"shortUrl",
		"starred",
		"status",
		"subscribed",
		"type",
		"unconfirmedBoardInvitation",
		"unconfirmedOrganizationInvitation",
		"updateBoard",
		"updateCard",
		"updateCard:closed",
		"updateCard:desc",
		"updateCard:idList",
		"updateCard:name",
		"updateCheckItemStateOnCard",
		"updateChecklist",
		"updateList",
		"updateList:closed",
		"updateList:name",
		"updateMember",
		"updateOrganization",
		"url",
		"username",
		"uses",
		"visible",
		"website"
	]

	PARAM_DEFAULT_FIELDS = [
		"active",
		"addAttachmentToCard",
		"addChecklistToCard",
		"addMemberToBoard",
		"addMemberToCard",
		"addMemberToOrganization",
		"addToOrganizationBoard",
		"admin",
		"admins",
		"avatarHash",
		"badges",
		"billableMemberCount",
		"bio",
		"bioData",
		"bytes",
		"checkItemStates",
		"closed",
		"color",
		"commentCard",
		"confirmed",
		"convertToCardFromCheckItem",
		"copyBoard",
		"copyCard",
		"copyCommentCard",
		"count",
		"createBoard",
		"createCard",
		"createList",
		"createOrganization",
		"data",
		"date",
		"dateLastActivity",
		"dateLastView",
		"deactivated",
		"deleteAttachmentFromCard",
		"deleteBoardInvitation",
		"deleteCard",
		"deleteOrganizationInvitation",
		"desc",
		"descData",
		"disablePowerUp",
		"displayName",
		"due",
		"edgeColor",
		"email",
		"emailCard",
		"enablePowerUp",
		"fullName",
		"idAttachmentCover",
		"idBoard",
		"idBoards",
		"idCard",
		"idChecklists",
		"idLabels",
		"idList",
		"idMember",
		"idMemberCreator",
		"idMembers",
		"idMembersVoted",
		"idOrganization",
		"idPremOrgsAdmin",
		"idShort",
		"initials",
		"invitations",
		"invited",
		"isUpload",
		"labelNames",
		"labels",
		"list",
		"logoHash",
		"makeAdminOfBoard",
		"makeNormalMemberOfBoard",
		"makeNormalMemberOfOrganization",
		"makeObserverOfBoard",
		"manualCoverAttachment",
		"me",
		"memberJoinedTrello",
		"memberType",
		"memberships",
		"mimeType",
		"mine",
		"minimal",
		"moveCardFromBoard",
		"moveCardToBoard",
		"moveListFromBoard",
		"moveListToBoard",
		"name",
		"normal",
		"open",
		"owners",
		"pinned",
		"pos",
		"powerUps",
		"prefs",
		"premiumFeatures",
		"previews",
		"products",
		"removeChecklistFromCard",
		"removeFromOrganizationBoard",
		"removeMemberFromCard",
		"shortLink",
		"shortUrl",
		"starred",
		"status",
		"subscribed",
		"type",
		"unconfirmedBoardInvitation",
		"unconfirmedOrganizationInvitation",
		"updateBoard",
		"updateCard",
		"updateCard:closed",
		"updateCard:desc",
		"updateCard:idList",
		"updateCard:name",
		"updateCheckItemStateOnCard",
		"updateChecklist",
		"updateList",
		"updateList:closed",
		"updateList:name",
		"updateMember",
		"updateOrganization",
		"url",
		"username",
		"uses",
		"visible",
		"website"
	]
end