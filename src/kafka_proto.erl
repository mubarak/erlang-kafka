%%% @doc
%%% Kafka protocol specific functions and utils.

%%% @author Aleksey Morarash <aleksey.morarash@proffero.com>
%%% @since 04 Aug 2014
%%% @copyright 2014, Proffero <info@proffero.com>

-module(kafka_proto).

%% API exports
-export(
   [encode/1,
    decode/2
   ]).

-include("kafka.hrl").
-include("kafka_proto.hrl").

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Encode a Kafka message for transmission.
-spec encode(RequestMessage :: request()) -> Packet :: binary().
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
-spec decode(RequestType :: api_key(),
             ResponsePacket :: binary()) ->
                    Message :: response().
decode(RequestType, ResponsePacket) ->
    <<
      CorellationId:32/big-signed,
      EncodedResponseMessage/binary
    >> = ResponsePacket,
    ResponseMessage =
        decode_response_payload(RequestType, EncodedResponseMessage),
    #response{corellation_id = CorellationId,
              message = ResponseMessage}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Encode request message payload.
-spec encode_message(ApiKey :: api_key(),
                     RequestPayload :: request_payload()) ->
                            binary().
encode_message(?MetadataRequest, Topics) ->
    encode_array(lists:map(fun encode_string/1, Topics));
encode_message(?ProduceRequest, Payload) ->
    EncodedTopics =
        encode_array(
          lists:map(
            fun({TopicName, Partitions}) ->
                    iolist_to_binary(
                      [encode_string(TopicName),
                       encode_produce_request_partitions(
                         Partitions)])
            end, Payload#produce_request.topics)),
    <<
      (Payload#produce_request.required_acks):16/big-signed,
      (Payload#produce_request.timeout):32/big-signed,
      EncodedTopics/binary
    >>.

%% @doc Encode partitions list for a topic in
%% Produce request payload.
-spec encode_produce_request_partitions(
        Partitions :: produce_request_partitions()) ->
                                               binary().
encode_produce_request_partitions(Partitions) ->
    encode_array(
      lists:map(
        fun({Partition, MessageSet}) ->
                EncodedMessageSet = encode_message_set(MessageSet),
                MessageSetSize = size(EncodedMessageSet),
                <<
                  Partition:32/big-signed,
                  MessageSetSize:32/big-signed,
                  EncodedMessageSet/binary
                >>
        end, Partitions)).

%% @doc Encode a MessageSet structure.
-spec encode_message_set(MessageSet :: message_set()) -> binary().
encode_message_set(MessageSet) ->
    iolist_to_binary(
      lists:map(
        fun({Offset, Message}) ->
                EncodedMessage = encode_message(Message),
                MessageSize = size(EncodedMessage),
                <<
                  Offset:64/big-signed,
                  MessageSize:32/big-signed,
                  EncodedMessage/binary
                >>
        end, MessageSet)).

%% @doc Encode a Message structure.
-spec encode_message(Message :: message()) -> binary().
encode_message(Message) ->
    EncodedKey = encode_bytes(Message#message.key),
    EncodedValue = encode_bytes(Message#message.value),
    CheckedEncodedMessage =
        <<
          (Message#message.magic_byte):8/big-signed,
          0:5,
          (Message#message.compression):3/big-unsigned,
          EncodedKey/binary,
          EncodedValue/binary
        >>,
    CRC = erlang:crc32(CheckedEncodedMessage),
    %% As defined in the protocol reference, the CRC field is
    %% encoded as signed 32bit integer. It can be true in
    %% some languages like Python which have zlib.crc32()
    %% function which returns signed 32bit integer, but this
    %% is not true in the Erlang: erlang:crc32/1 function
    %% returns unsigned 32bit integer, so we encode it here as
    %% unsigned too.
    <<CRC:32/big-unsigned, CheckedEncodedMessage/binary>>.

%% @doc Encode array. The array items must be already encoded.
-spec encode_array(EncodedItems :: [binary()]) ->
                          EncodedArray :: binary().
encode_array(EncodedItems) ->
    Length = length(EncodedItems),
    iolist_to_binary([<<Length:32/big-signed>> | EncodedItems]).

%% @doc Encode string.
-spec encode_string(kafka_string()) -> binary().
encode_string(?NULL) ->
    Length = -1,
    <<Length:16/big-signed>>;
encode_string(String) when is_list(String) ->
    Binary = list_to_binary(String),
    BinaryLen = size(Binary),
    <<BinaryLen:16/big-signed, Binary/binary>>.

-spec encode_bytes(bytes()) -> binary().
encode_bytes(?NULL) ->
    Length = -1,
    <<Length:32/big-signed>>;
encode_bytes(Binary) when is_binary(Binary) ->
    Length = size(Binary),
    <<Length:32/big-signed, Binary/binary>>.

%% @doc Decode response message payload.
-spec decode_response_payload(RequestType :: api_key(),
                              EncodedMessage :: binary()) ->
                                     metadata_response().
decode_response_payload(?MetadataRequest, EncodedMessage) ->
    {Brokers, Tail} = decode_broker_array(EncodedMessage),
    Topics = decode_topic_array(Tail),
    #metadata_response{brokers = Brokers,
                       topics = Topics};
decode_response_payload(?ProduceRequest, EncodedMessage) ->
    <<Length:32/big-signed, EncodedItems/binary>> = EncodedMessage,
    decode_produce_array_loop(Length, EncodedItems).

%% @doc Decode array items of produce response payload one by one.
-spec decode_produce_array_loop(Length :: non_neg_integer(),
                                EncodedItems :: binary()) ->
                                       produce_response().
decode_produce_array_loop(0, <<>>) ->
    [];
decode_produce_array_loop(Length, EncodedItems)
  when Length > 0 ->
    {TopicName, EncodedPartitionsArray} =
        decode_string(EncodedItems),
    {Partitions, FinalTail} =
        decode_produce_partitions_array(EncodedPartitionsArray),
    [{TopicName, Partitions} |
     decode_produce_array_loop(Length - 1, FinalTail)].

%% @doc Decode partitions array of produce response payload.
-spec decode_produce_partitions_array(Encoded :: binary()) ->
                                             {produce_response_partitions(),
                                              Tail :: binary()}.
decode_produce_partitions_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_produce_partitions_array_loop(Length, EncodedItems, []).

%% @doc Decode partitions array items of produce response payload
%% one by one.
-spec decode_produce_partitions_array_loop(
        Length :: non_neg_integer(),
        EncodedItems :: binary(),
        Accum :: produce_response_partitions()) ->
             {Partitions :: produce_response_partitions(),
              Tail :: binary()}.
decode_produce_partitions_array_loop(0, Tail, Accum) ->
    {lists:reverse(Accum), Tail};
decode_produce_partitions_array_loop(Length, Encoded, Accum)
  when Length > 0 ->
    <<
      PartitionId:32/big-signed,
      ErrorCode:16/big-signed,
      Offset:64/big-signed,
      Tail/binary
    >> = Encoded,
    decode_produce_partitions_array_loop(
      Length - 1, Tail, [{PartitionId, ErrorCode, Offset} | Accum]).

%% @doc MetadataResponse: Decode an array of brokers.
-spec decode_broker_array(Encoded :: binary()) ->
                                 {metadata_brokers(),
                                  Tail :: binary()}.
decode_broker_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_broker_array_loop(Length, EncodedItems, []).

%% @doc Decode broker array items one by one.
-spec decode_broker_array_loop(Length :: non_neg_integer(),
                               EncodedItems :: binary(),
                               Accum :: metadata_brokers()) ->
                                      {metadata_brokers(),
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
-spec decode_topic_array(Encoded :: binary()) -> metadata_topics().
decode_topic_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_topic_array_loop(Length, EncodedItems).

%% @doc Decode topic array items one by one.
-spec decode_topic_array_loop(Length :: non_neg_integer(),
                              EncodedItems :: binary()) ->
                                     Topics :: metadata_topics().
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
                                             {metadata_partitions(),
                                              Tail :: binary()}.
decode_partition_metadata_array(Encoded) ->
    <<Length:32/big-signed, EncodedItems/binary>> = Encoded,
    decode_partition_metadata_array_loop(Length, EncodedItems, []).

%% @doc Decode metadata partitions array items one by one.
-spec decode_partition_metadata_array_loop(
        Length :: non_neg_integer(),
        EncodedItems :: binary(),
        Accum :: metadata_partitions()) -> {metadata_partitions(),
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
                           {Decoded :: kafka_string(),
                            Rest :: binary()}.
decode_string(Encoded) ->
    <<Length:16/big-signed, Rest/binary>> = Encoded,
    if Length == -1 ->
            {?NULL, Rest};
       true ->
            {Subject, FinalRest} = split_binary(Rest, Length),
            {binary_to_list(Subject), FinalRest}
    end.
