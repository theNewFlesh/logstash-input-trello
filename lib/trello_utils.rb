# encoding: utf-8

require 'uri'
require 'net/http'
require 'json'
require 'set'

module TrelloUtils
	class TrelloClient
		attr_reader(:host, :port, :organizations, :key, :token, :board_ids,
			:parameters, :filters, :fields, :entities)

		def initialize(kwargs={
							organizations:  [], # an array of organizations from which to derive board ids
							key:            nil, # oauth key
							token:          nil, # oauth secret
							board_ids:      [],
							fields:         PARAM_DEFAULT_FIELDS,
							entities:       PARAM_DEFAULT_ENTITIES,
							filters:        {},
							port:           443 # trello REST port
			})

			@organizations = kwargs[:organizations]
			@key           = kwargs[:key]
			@token         = kwargs[:token]
			@board_ids     = kwargs[:board_ids]
			@fields        = kwargs[:fields]
			@entities      = kwargs[:entities]
			@filters       = kwargs[:filters]
			@port          = kwargs[:port]	

			# get board ids if none are provided
			if @board_ids.empty?
				board_filter = "all"
				if @filters.has_key?("boards")
					if not @filters["boards"].empty?
						board_filter = array_to_uri( @filters["boards"] )
					end
				end
				@board_ids = get_board_ids(@board_ids, board_filter)
			end

			# create fields hash
			@fields = Set.new(@fields)
			new_fields = ALL_FIELDS.clone
			new_fields.to_a.each do |key, val|
				temp = @fields.intersection(val).to_a
	            new_fields[key] = array_to_uri(temp)
			end
			@fields = new_fields

			# create entities hash
			new_entities = {}
	        @entities.each do |entity|
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
			@filters.each do |key, val|
				if new_entities.include?(key)
					if val != "none"
						new_entities[key] = array_to_uri(filters[entity])
					end
				end
			end
			@entities = new_entities

			# merge fields and entities into params
			params = new_fields.merge(new_entities)
			
			# switch out board_fields with fields
			params["fields"] = params["board_fields"]
			params.delete("board_fields")

			@parameters = params
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

		# get board ids
		private
		def get_board_ids(board_ids, board_filter)
			if not board_ids.empty?
				return board_ids
			else
				board_ids = Set.new()
				@organizations.each do |org|
					uri =  "/1/organizations/#{org}/boards/#{board_filter}?"
					uri += URI.encode_www_form({key: @key, token: @token})
					response = issue_request(uri)
					response.each do |item|
						board_ids.add(item["shortLink"])
					end
				end
				return board_ids.to_a
			end
		end

		# construct uri
		public
		def get_uri(board_id, actions_since)
			uri = "/1/boards/#{board_id}?"
			args = {
				"actions_format" => "list",
				"actions_since"  => actions_since,
				"actions_limit"  => "1000",
				"labels_limit"   => "1000",
				"key"            => @key,
				"token"          => @token
			}
			uri += URI.encode_www_form(@parameters.merge(args))
			return uri
		end

		public
		def issue_request(uri)
			response = Net::HTTP.new("api.trello.com", @port)
			response.use_ssl = true
			response = response.request_get(uri, {"Connection" => "close"})
			code = response.code.to_i()
			if 199 < code and code < 300
				return JSON.load(response.body)
			else
				@logger.warn("HTTP request error: ", + response)
				raise RuntimeError, "HTTP request failed with code #{code}: #{response}"
			end
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

	PARAM_DEFAULT_ENTITIES = [
		# "actions_entities",
		"action_member",
		"action_memberCreator",
		# "card_stickers",
		# "memberships_member",
		# "organization",
		# "myPrefs",
		# "card_attachments",
		"cards",
		"card_checklists",
		# "boardStars",
		"labels",
		"lists",
		"members",
		# "membersInvited",
		"checklists"
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
		# "avatarHash",
		# "badges",
		"billableMemberCount",
		"bio",
		# "bioData",
		"bytes",
		"checkItemStates",
		"closed",
		"color",
		"commentCard",
		# "confirmed",
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
		# "descData",
		"disablePowerUp",
		"displayName",
		"due",
		# "edgeColor",
		# "email",
		# "emailCard",
		# "enablePowerUp",
		"fullName",
		# "idAttachmentCover",
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
		# "idPremOrgsAdmin",
		# "idShort",
		# "initials",
		# "invitations",
		# "invited",
		"isUpload",
		"labelNames",
		"labels",
		"list",
		# "logoHash",
		"makeAdminOfBoard",
		"makeNormalMemberOfBoard",
		"makeNormalMemberOfOrganization",
		"makeObserverOfBoard",
		"manualCoverAttachment",
		"me",
		"memberJoinedTrello",
		# "memberType",
		# "memberships",
		# "mimeType",
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
		# "pinned",
		"pos",
		# "powerUps",
		# "prefs",
		"premiumFeatures",
		"previews",
		# "products",
		"removeChecklistFromCard",
		"removeFromOrganizationBoard",
		"removeMemberFromCard",
		"shortLink",
		# "shortUrl",
		# "starred",
		"status",
		# "subscribed",
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
		# "url",
		"username",
		"uses",
		"visible",
		"website"
	]
end