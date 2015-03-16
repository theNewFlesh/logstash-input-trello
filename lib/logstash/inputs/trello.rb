# encoding: utf-8

require "logstash/inputs/base"
require "logstash/namespace"
require "stud/interval"
require "socket" # for Socket.gethostname
require 'addressable/uri'
require 'net/http'
require 'json'
require 'time'

# The trello filter is used querying the trello database and returning the resulting
# cards as events

class LogStash::Inputs::Trello < LogStash::Inputs::Base
	config_name "trello"
	milestone 1

	default :codec, "json_lines"
	# defualt: json_lines

	config(:interval, :validate => :number, :default => 1800)
	# query interval
	# default: 30 minutes

	config(:key, :validate => :string, :required => true)
	# oauth key

	config(:token, :validate => :string, :required => true)
	# oauth secret

	config(:query, :validate => :hash, :required => true)
	# Valid Values:
	# 	a string with a length from 1 to 16384
	
	config(:id_boards, :default => "mine")
	# Valid Values:
	# 	A comma-separated list of objectIds, 24-character hex strings

	config(:id_organizations, :validate => :array)
	# Valid Values:
	# 	A comma-separated list of objectIds, 24-character hex strings
	
	config(:id_cards)
	# Valid Values:
	# 	A comma-separated list of objectIds, 24-character hex strings
	
	config(:model_types, :default => "all")
	# Valid Values:
	# 	actions
	# 	boards
	# 	cards
	# 	members
	# 	organizations

	config(:board_fields, :default => ["name", "idOrganization"])
	# Valid Values:
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

	config(:boards_limit, :default => 10)
	# Valid Values:
	# 	a number from 1 to 1000

	config(:card_fields, :vailidate => :array, :default => ["all"])
	# Valid Values:
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

	config(:cards_limit, :default => 10)
	# Valid Values:
	# 	a number from 1 to 1000

	config(:cards_page, :default => 0)
	# Valid Values:
	# 	a number from 0 to 100

	config(:card_board, :default => false)
	# Valid Values:
	# 	true
	# 	false

	config(:card_list, :default => false)
	# Valid Values:
	# 	true
	# 	false

	config(:card_members, :default => false)
	# Valid Values:
	# 	true
	# 	false

	config(:card_stickers, :default => false)
	# Valid Values:
	# 	true
	# 	false

	config(:card_attachments, :default => false)
	# Valid Values:
	# 	A boolean value or &quot;cover&quot; for only card cover attachments

	config(:organization_fields, :default => ["name", "displayName"])
	# Valid Values:
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

	config(:organizations_limit, :default => 10)
	# Valid Values:
	# 	a number from 1 to 1000

	config(:member_fields, :default => ["avatarHash", "fullName", "initials", "username", "confirmed"])
	# Valid Values:
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

	config(:members_limit, :default => 10)
	# Valid Values:
	# 	a number from 1 to 1000

	config(:partial, :default => false)
	# Valid Values:
	# 	true
	# 	false

	# def initialize(*args)
	# 	super(*args)
	# end

	public
	def register()
		@host = Socket.gethostname
	end

	private
	def get_uri(page)
		query_url = ""
		@query.each do |key, val|
			query_url += key + "%3A" + val + "+"
		end

		uri =  "/1/search?"
		uri += "query=" + query_url
		uri += "&modelTypes=cards"
		
		# card options
		uri += "&cards_limit=1000"
		uri += "&cards_page=" + page.to_s()
		uri += "&card_members=true"
		uri += "&card_board=true"
		uri += "&card_list=true"
		uri += "&card_fields=" + @card_fields.join(",")
		
		# board options
		uri += "&boards_limit=1000"
		uri += "&board_fields=name"
		
		uri += "&key=" + @key
		uri += "&token=" + @token
		return uri
	end

	private
	def coerce_nulls(data)
		data.each do |key, val|
			if val == ""
				data[key] = nil
			end
		end
		return data
	end

	public
	def run(queue)
		# begin
		Stud.interval(@interval) do
			(0..1000).each do |page|
				uri = get_uri(page)

				response = Net::HTTP.new("api.trello.com", 443)
				response.use_ssl = true
				response = response.request_get(uri, {'Connection' => 'close'})
				code = response.code.to_i()
				if 199 < code and code < 300
					response = JSON.parse(response.body())
					response = coerce_nulls(response)
					if response["cards"].length > 0
						# add events to queue
						response["cards"].each do |data|
							event = LogStash::Event.new("data" => data, "host" => @host)
							decorate(event)
							queue << event
						end
					end
					if response["cards"].length == 1000
						next
					else
						break
					end
				else
					raise NET::HTTPError.new(response.body())
				end
			end
		end
		# rescue Logstash::ShutdownSignal
		# 	event.cancel()
		# 	@logger.debug(e)
		# end
	end
end