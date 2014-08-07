%%%----------------------------------------------------------------------
%%% File        : kafka_proto.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@proffero.com>
%%% Description : Kafka Protocol definitions
%%% Created     : 04 Aug 2014
%%%----------------------------------------------------------------------

-ifndef(_KAFKA_PROTO).
-define(_KAFKA_PROTO, true).

%% ----------------------------------------------------------------------
%% Registered values for the protocol structure fields
%% ----------------------------------------------------------------------

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
-define(NULL, null).

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

%% ----------------------------------------------------------------------
%% Protocol Primitive Types
%% ----------------------------------------------------------------------

%% signed int8
-type int8() :: -128..127.

%% signed int16
-type int16() :: -32768..32767.

%% signed int32
-type int32() :: -2147483648..2147483647.

%% signed int64
-type int64() :: -9223372036854775808..9223372036854775807.

-type kafka_string() :: string() | ?NULL.

-type bytes() :: binary() | ?NULL.

%% ----------------------------------------------------------------------
%% Request definitions
%% ----------------------------------------------------------------------

-record(
   request,
   {
     %% This is a numeric id for the API being invoked
     %% (i.e. is it a metadata request, a produce request,
     %% a fetch request, etc).
     api_key :: api_key(),
     %% This is a numeric version number for this api. We version
     %% each API and this version number allows the server to
     %% properly interpret the request as the protocol evolves.
     %% Responses will always be in the format corresponding to
     %% the request version. Currently the supported version
     %% for all APIs is 0.
     api_version = ?ApiVersion :: api_version(),
     %% This is a user-supplied integer. It will be passed back
     %% in the response by the server, unmodified. It is useful
     %% for matching request and response between the client
     %% and server.
     corellation_id = 0 :: corellation_id(),
     %% This is a user supplied identifier for the client application.
     %% The user can use any identifier they like and it will be used
     %% when logging errors, monitoring aggregates, etc. For example,
     %% one might want to monitor not just the requests per second
     %% overall, but the number coming from each client application
     %% (each of which could reside on multiple servers). This id acts
     %% as a logical grouping across all requests from a particular
     %% client.
     client_id :: client_id(),
     message :: request_payload()
   }).

-type request() :: #request{}.

-type api_key() ::
        ?ProduceRequest | ?FetchRequest | ?OffsetRequest |
        ?MetadataRequest | ?OffsetCommitRequest |
        ?OffsetFetchRequest | ?ConsumerMetadataRequest.

-type api_version() :: int16().

-type client_id() :: string().

-type request_payload() ::
        metadata_request() | produce_request().

%% ----------------------------------------------------------------------
%% Response definitions
%% ----------------------------------------------------------------------

-record(
   response,
   {corellation_id :: corellation_id(),
    message :: response_payload()
   }).

-type response() :: #response{}.

-type corellation_id() :: int32().

-type response_payload() ::
        metadata_response() | produce_response().

%% ----------------------------------------------------------------------
%% Metadata API: request definitions
%% ----------------------------------------------------------------------

-type metadata_request() ::
        [TopicName :: topic_name()].

-type topic_name() :: nonempty_string().

%% ----------------------------------------------------------------------
%% Metadata API: reply definitions
%% ----------------------------------------------------------------------

-record(
   metadata_response,
   {brokers = [] :: metadata_brokers(),
    topics = [] :: metadata_topics()
   }).

-type metadata_response() :: #metadata_response{}.

-type metadata_brokers() :: [metadata_brokers_item()].

-type metadata_brokers_item() ::
        {NodeId :: broker_id(),
         Host :: string(),
         Port :: inet:port_number()}.

-type broker_id() :: int32().

-type metadata_topics() :: [metadata_topics_item()].

-type metadata_topics_item() ::
        {TopicErrorCode :: error_code(),
         TopicName :: topic_name(),
         metadata_partitions()}.

-type error_code() :: int16().

-type metadata_partitions() :: [metadata_partitions_item()].

-type metadata_partitions_item() ::
        {PartitionErrorCode :: error_code(),
         PartitionId :: partition_id(),
         Leader :: broker_id(),
         Replicas :: [broker_id()],
         Isr :: [broker_id()]}.

-type partition_id() :: int32().

%% ----------------------------------------------------------------------
%% Produce API: request definitions
%% ----------------------------------------------------------------------

-record(
   produce_request,
   {
     %% This field indicates how many acknowledgements the servers
     %% should receive before responding to the request. If it is 0
     %% the server will not send any response (this is the only case
     %% where the server will not reply to a request). If it is 1, the
     %% server will wait the data is written to the local log before
     %% sending a response. If it is -1 the server will block until
     %% the message is committed by all in sync replicas before sending
     %% a response. For any number > 1 the server will block waiting
     %% for this number of acknowledgements to occur (but the server
     %% will never wait for more acknowledgements than there are
     %% in-sync replicas).
     required_acks :: produce_request_required_acks(),
     %% This provides a maximum time in milliseconds the server can
     %% await the receipt of the number of acknowledgements in
     %% RequiredAcks. The timeout is not an exact limit on the request
     %% time for a few reasons: (1) it does not include network latency,
     %% (2) the timer begins at the beginning of the processing of this
     %% request so if many requests are queued due to server overload
     %% that wait time will not be included, (3) we will not terminate
     %% a local write so if the local write time exceeds this timeout
     %% it will not be respected. To get a hard timeout of this type
     %% the client should use the socket timeout.
     timeout :: produce_request_timeout(),
     topics :: produce_request_topics()
   }).

-type produce_request() :: #produce_request{}.

-type produce_request_required_acks() :: -1..32767. %% a subset of int16

-type produce_request_timeout() :: 0..2147483647. %% a subset of int32

-type produce_request_topics() :: [produce_request_topics_item()].

-type produce_request_topics_item() ::
        {
          %% The topic that data is being published to.
          TopicName :: topic_name(),
          Partitions :: produce_request_partitions()}.

-type produce_request_partitions() :: [produce_request_partitions_item()].

-type produce_request_partitions_item() ::
        {
          %% The partition that data is being published to.
          Partition :: partition_id(),
          %% A set of messages in the standard format.
          MessageSet :: message_set()}.

-type message_set() :: [message_set_item()].

-type message_set_item() ::
        {
          %% This is the offset used in kafka as the log sequence
          %% number. When the producer is sending messages it doesn't
          %% actually know the offset and can fill in any value here
          %% it likes.
          Offset :: offset(),
          Message :: message()
        }.

-type offset() :: int64().

-record(
   message,
   {
     %% This is a version id used to allow backwards compatible
     %% evolution of the message binary format.
     magic_byte = 0 :: magic_byte(),
     %% Kafka supports compressing messages for additional efficiency,
     %% however this is more complex than just compressing a raw message.
     %% Because individual messages may not have sufficient redundancy
     %% to enable good compression ratios, compressed messages must be
     %% sent in special batches (although you may use a batch of one if
     %% you truly wish to compress a message on its own). The messages
     %% to be sent are wrapped (uncompressed) in a MessageSet structure,
     %% which is then compressed and stored in the Value field of a
     %% single "Message" with the appropriate compression codec set.
     %% The receiving system parses the actual MessageSet from the
     %% decompressed value. Kafka currently supports two compression
     %% codecs with the following codec numbers: None - 0, GZIP - 1,
     %% Snappy - 2.
     compression = ?None :: compression(),
     %% The key is an optional message key that was used for partition
     %% assignment. The key can be null.
     key = ?NULL :: key(),
     %% The value is the actual message contents as an opaque byte array.
     %% Kafka supports recursive messages in which case this may itself
     %% contain a message set. The message can be null.
     value = ?NULL :: value()
   }).

-type message() :: #message{}.

-type magic_byte() :: int8().

-type compression() :: ?None | ?GZIP | ?Snappy.

-type key() :: bytes().

-type value() :: bytes().

%% ----------------------------------------------------------------------
%% Produce API: response definitions
%% ----------------------------------------------------------------------

-type produce_response() :: [produce_response_item()].

-type produce_response_item() ::
        {TopicName :: topic_name(),
         Partitions :: produce_response_partitions()}.

-type produce_response_partitions() :: [produce_response_partitions_item()].

-type produce_response_partitions_item() ::
        {
          %% The partition this response entry corresponds to.
          Partition :: partition_id(),
          %% The error from this partition, if any. Errors are given
          %% on a per-partition basis because a given partition may be
          %% unavailable or maintained on a different host, while
          %% others may have successfully accepted the produce request.
          ErrorCode :: error_code(),
          %% The offset assigned to the first message in the message
          %% set appended to this partition.
          Offset :: offset()}.

-endif.
