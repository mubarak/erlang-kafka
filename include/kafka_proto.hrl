%%%----------------------------------------------------------------------
%%% File        : kafka_proto.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@proffero.com>
%%% Description : Kafka Protocol definitions
%%% Created     : 04 Aug 2014
%%%----------------------------------------------------------------------

-ifndef(_KAFKA_PROTO).
-define(_KAFKA_PROTO, true).

%% API Keys
-define(ProduceRequest, 0).
-define(FetchRequest, 1).
-define(OffsetRequest, 2).
-define(MetadataRequest, 3).
%% 4-7: Non-user facing control APIs
-define(OffsetCommitRequest, 8).
-define(OffsetFetchRequest, 9).
-define(ConsumerMetadataRequest, 10).

%% Current API version
-define(ApiVersion, 0).

%% Compression levels
-define(None, 0).
-define(GZIP, 1).
-define(Snappy, 2).

%% NULL value
-define(NULL, '*null*').

-record(
   message_set,
   {offset :: kafka_proto:int64(),
    message :: any()
   }).

-record(
   message,
   {compression :: kafka_proto:compression(),
    %% The key is an optional message key that was used for partition
    %% assignment. The key can be null.
    key :: binary(),
    %% The value is the actual message contents as an opaque byte array.
    %% Kafka supports recursive messages in which case this may itself
    %% contain a message set. The message can be null.
    value :: any()
   }).

-record(
   request,
   {
     %% This is a numeric id for the API being invoked
     %% (i.e. is it a metadata request, a produce request,
     %% a fetch request, etc).
     api_key :: kafka_proto:api_key(),
     %% This is a numeric version number for this api. We version
     %% each API and this version number allows the server to
     %% properly interpret the request as the protocol evolves.
     %% Responses will always be in the format corresponding to
     %% the request version. Currently the supported version
     %% for all APIs is 0.
     api_version = ?ApiVersion :: kafka_proto:int16(),
     %% This is a user-supplied integer. It will be passed back
     %% in the response by the server, unmodified. It is useful
     %% for matching request and response between the client
     %% and server.
     corellation_id :: kafka_proto:int32(),
     %% This is a user supplied identifier for the client application.
     %% The user can use any identifier they like and it will be used
     %% when logging errors, monitoring aggregates, etc. For example,
     %% one might want to monitor not just the requests per second
     %% overall, but the number coming from each client application
     %% (each of which could reside on multiple servers). This id acts
     %% as a logical grouping across all requests from a particular
     %% client.
     client_id :: string(),
     message :: #message{} | #message_set{} | string()
   }).

-record(
   metadata_response,
   {brokers = [] :: kafka_proto:brokers(),
    topics = [] :: kafka_proto:topics()
   }).

-record(
   response,
   {corellation_id :: kafka_proto:int32(),
    message :: #message{} | #message_set{} | #metadata_response{}
   }).

%% ----------------------------------------------------------------------
%% Error codes

%% No error -- it worked!
-define(NoError, 0).
%% An unexpected server error
-define(Unknown, -1).
%% The requested offset is outside the range of offsets maintained by the
%% server for the given topic/partition
-define(OffsetOutOfRange, 1).
%% This indicates that a message contents does not match its CRC
-define(InvalidMessage, 2).
%% This request is for a topic or partition that does not exist on this
%% broker
-define(UnknownTopicOrPartition, 3).
%% The message has a negative size
-define(InvalidMessageSize, 4).
%% This error is thrown if we are in the middle of a leadership election
%% and there is currently no leader for this partition and hence it is
%% unavailable for writes
-define(LeaderNotAvailable, 5).
%% This error is thrown if the client attempts to send messages to
%% a replica that is not the leader for some partition. It indicates
%% that the clients metadata is out of date
-define(NotLeaderForPartition, 6).
%% This error is thrown if the request exceeds the user-specified time
%% limit in the request
-define(RequestTimedOut, 7).
%% This is not a client facing error and is used only internally by
%% intra-cluster broker communication
-define(BrokerNotAvailable, 8).
-define(Unused, 9). %% Unused
%% The server has a configurable maximum message size to avoid unbounded
%% memory allocation. This error is thrown if the client attempt to
%% produce a message larger than this maximum
-define(MessageSizeTooLarge, 10).
%% Internal error code for broker-to-broker communication
-define(StaleControllerEpochCode, 11).
%% If you specify a string larger than configured maximum for offset
%% metadata
-define(OffsetMetadataTooLargeCode, 12).
%% The broker returns this error code for an offset fetch request if
%% it is still loading offsets (after a leader change for that offsets
%% topic partition)
-define(OffsetsLoadInProgressCode, 14).
%% The broker returns this error code for consumer metadata requests
%% or offset commit requests if the offsets topic has not yet been
%% created
-define(ConsumerCoordinatorNotAvailableCode, 15).
%% The broker returns this error code if it receives an offset fetch
%% or commit request for a consumer group that it is not a coordinator for
-define(NotCoordinatorForConsumerCode, 16).

-endif.
