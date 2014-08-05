%%% @doc
%%% Kafka protocol specific functions and utils.

%%% @author Aleksey Morarash <aleksey.morarash@proffero.com>
%%% @since 04 Aug 2014
%%% @copyright 2014, Proffero <info@proffero.com>

-module(kafka_proto).

%% API exports
-export(
   [encode/1,
    decode/1
   ]).

-include("kafka.hrl").
-include("kafka_proto.hrl").

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-export_types(
   [int16/0,
    int32/0,
    int64/0,
    api_key/0,
    compression/0,
    brokers/0,
    broker/0,
    broker_id/0,
    topics/0,
    topic/0,
    partition_metadata/0
   ]).

%% signed int16
-type int16() :: -32768..32767.

%% signed int32
-type int32() :: -2147483648..2147483647.

%% signed int64
-type int64() :: -9223372036854775808..9223372036854775807.

-type api_key() ::
        ?ProduceRequest | ?FetchRequest | ?OffsetRequest |
        ?MetadataRequest | ?OffsetCommitRequest |
        ?OffsetFetchRequest | ?ConsumerMetadataRequest.

-type compression() :: ?None | ?GZIP | ?Snappy.

-type brokers() :: [broker()].

-type broker() ::
        {NodeId :: broker_id(),
         Host :: string(),
         Port :: int32()}.

-type broker_id() :: int32().

-type topics() :: [topic()].

-type topic() :: {TopicErrorCode :: int16(),
                  TopicName :: string(),
                  [partition_metadata()]}.

-type partition_metadata() ::
        {PartitionErrorCode :: int16(),
         PartitionId :: int32(),
         Leader :: int32(),
         Replicas :: [int32()],
         Isr :: [int32()]}.

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Encode a Kafka message for transmission.
-spec encode(RequestMessage :: #request{}) -> Packet :: binary().
encode(RequestMessage) ->
    EncodedClientId = encode_string(RequestMessage#request.client_id),
    EncodedPayload =
        encode_message(
          RequestMessage#request.api_key,
          RequestMessage#request.message),
    <<
      (RequestMessage#request.api_key):16/big-signed,
      (RequestMessage#request.api_version):16/big-signed,
      (RequestMessage#request.corellation_id):32/big-signed,
      EncodedClientId/binary,
      EncodedPayload/binary
    >>.

%% @doc Decode a Kafka response from packet received from network.
-spec decode(ResponsePacket :: binary()) -> Message :: #response{}.
decode(ResponsePacket) ->
    <<
      CorellationId:32/big-signed,
      EncodedResponseMessage/binary
    >> = ResponsePacket,
    ResponseMessage = decode_message(EncodedResponseMessage),
    #response{corellation_id = CorellationId,
              message = ResponseMessage}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Encode request message payload.
-spec encode_message(ApiKey :: api_key(),
                     #message{} |
                     #message_set{} |
                     (MetadataRequest :: [string()])) ->
                            binary().
encode_message(?MetadataRequest, Topics) ->
    encode_array(lists:map(fun encode_string/1, Topics)).

%% @doc Encode array. The array items must be already encoded.
-spec encode_array(EncodedItems :: [binary()]) ->
                          EncodedArray :: binary().
encode_array(EncodedItems) ->
    Length = length(EncodedItems),
    iolist_to_binary([<<Length:32/big-signed>> | EncodedItems]).

%% @doc Encode string.
-spec encode_string(string()) -> binary().
encode_string(String) ->
    Binary = list_to_binary(String),
    BinaryLen = size(Binary),
    <<BinaryLen:16/big-signed, Binary/binary>>.

%% @doc Decode response message payload.
-spec decode_message(EncodedMessage :: binary()) ->
                            #metadata_response{}.
decode_message(EncodedMessage) ->
    {Brokers, Tail} = decode_broker_array(EncodedMessage),
    Topics = decode_topic_array(Tail),
    #metadata_response{brokers = Brokers,
                       topics = Topics}.

%% @doc MetadataResponse: Decode an array of brokers.
-spec decode_broker_array(Encoded :: binary()) ->
                                 {brokers(), Tail :: binary()}.
decode_broker_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_broker_array_loop(Length, EncodedItems, []).

%% @doc Decode broker array items one by one.
-spec decode_broker_array_loop(Length :: non_neg_integer(),
                               EncodedItems :: binary(),
                               Accum :: brokers()) ->
                                      {brokers(),
                                       Tail :: binary()}.
decode_broker_array_loop(0, Tail, Accum) ->
    {lists:reverse(Accum), Tail};
decode_broker_array_loop(Length, EncodedItems, Accum)
  when Length > 0 ->
    <<NodeId:32/big-signed, Tail/binary>> = EncodedItems,
    {Host, <<Port:32/big-signed, FinalTail/binary>>} =
        decode_string(Tail),
    decode_broker_array_loop(
      Length - 1, FinalTail, [{NodeId, Host, Port} | Accum]).

%% @doc MetadataResponse: Decode an array of topics.
-spec decode_topic_array(Encoded :: binary()) -> topics().
decode_topic_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_topic_array_loop(Length, EncodedItems).

%% @doc Decode topic array items one by one.
-spec decode_topic_array_loop(Length :: non_neg_integer(),
                              EncodedItems :: binary()) ->
                                     Topics :: topics().
decode_topic_array_loop(0, <<>>) ->
    [];
decode_topic_array_loop(Length, EncodedItems)
  when Length > 0 ->
    <<TopicErrorCode:16/big-signed, Tail0/binary>> = EncodedItems,
    {TopicName, Tail1} = decode_string(Tail0),
    {PartitionMetadata, FinalTail} =
        decode_partition_metadata_array(Tail1),
    [{TopicErrorCode, TopicName, PartitionMetadata} |
     decode_topic_array_loop(Length - 1, FinalTail)].

%% @doc Decode partition metadata.
-spec decode_partition_metadata_array(Encoded :: binary()) ->
                                             {[partition_metadata()],
                                              Tail :: binary()}.
decode_partition_metadata_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_partition_metadata_array_loop(Length, EncodedItems, []).

%% @doc Decode partition metadata array items one by one.
-spec decode_partition_metadata_array_loop(
        Length :: non_neg_integer(),
        EncodedItems :: binary(),
        Accum :: [partition_metadata()]) -> {[partition_metadata()],
                                             Tail :: binary()}.
decode_partition_metadata_array_loop(0, Tail, Accum) ->
    {lists:reverse(Accum), Tail};
decode_partition_metadata_array_loop(Length, EncodedItems, Accum)
  when Length > 0 ->
    <<PartitionErrorCode:16/big-signed,
      PartitionId:32/big-signed,
      Leader:32/big-signed,
      Tail0/binary>> = EncodedItems,
    {Replicas, Tail1} = decode_int32_array(Tail0),
    {Isr, FinalTail} = decode_int32_array(Tail1),
    decode_partition_metadata_array_loop(
      Length - 1, FinalTail,
      [{PartitionErrorCode, PartitionId, Leader,
        Replicas, Isr} | Accum]).

%% @doc Decode an array of int32.
-spec decode_int32_array(Encoded :: binary()) ->
                                {[int32()], Tail :: binary()}.
decode_int32_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_int32_array_loop(Length, EncodedItems, _Accum = []).

%% @doc Decode int32-items of an array one by one.
-spec decode_int32_array_loop(Length :: non_neg_integer(),
                              EncodedItems :: binary(),
                              Accum :: [int32()]) ->
                                     {[int32()], Tail :: binary()}.
decode_int32_array_loop(0, Tail, Accum) ->
    {lists:reverse(Accum), Tail};
decode_int32_array_loop(Length, EncodedItems, Accum)
  when Length > 0 ->
    <<Item:32/big-signed, Tail/binary>> = EncodedItems,
    decode_int32_array_loop(Length - 1, Tail, [Item | Accum]).

%% @doc Decode string.
-spec decode_string(Encoded :: binary()) ->
                           {Decoded :: string() | ?NULL,
                            Rest :: binary()}.
decode_string(Encoded) ->
    <<Length:16/big-signed, Rest/binary>> = Encoded,
    if Length == -1 ->
            {?NULL, Rest};
       true ->
            {Subject, FinalRest} = split_binary(Rest, Length),
            {binary_to_list(Subject), FinalRest}
    end.
