%%% @doc
%%% Kafka client implementation.
%%% Keeps TCP connections to Kafka brokers and handles
%%% request-reply interaction from the client owner.

%%% @author Aleksey Morarash <aleksey.morarash@proffero.com>
%%% @since 04 Aug 2014
%%% @copyright 2014, Proffero <info@proffero.com>

-module(kafka_client).

%% API exports
-export(
   [start_link/2,
    start_link/3,
    stop/1,
    hup/1,
    metadata/1,
    produce/5,
    produce_async/3
   ]).

%% gen_server callback exports
-export([init/1, handle_call/3, handle_info/2, handle_cast/2,
         terminate/2, code_change/3]).

-include("kafka.hrl").
-include("kafka_proto.hrl").

%% --------------------------------------------------------------------
%% Internal signals
%% --------------------------------------------------------------------

%% to tell the client to stop
-define(STOP, '*stop').

%% to tell the client to reconnect and reread metadata
-define(HUP, '*hup*').

%% to tell the client to reconnect the disconnected brokers
-define(RECONNECT, '*reconnect*').

%% to make a metadata request
-define(METADATA, '*metadata*').

%% to make a produce request
-define(PRODUCE, '*produce*').

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-type socket_item() ::
        {BrokerId :: broker_id(),
         Socket :: port() | undefined}.

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start the linked process.
-spec start_link(Brokers :: [kafka:broker()],
                 Options :: [kafka:option()]) ->
                        {ok, Pid :: pid()} |
                        {error, Reason :: any()}.
start_link(Brokers, Options) ->
    gen_server:start_link(
      ?MODULE, _Args = {Brokers, Options}, _Options = []).

%% @doc Start the named linked process.
%% Purpose of ServerName argument you can find at
%% the gen_server:start_link/4 function description.
-spec start_link(ServerName :: kafka:server_name(),
                 Brokers :: [kafka:broker()],
                 Options :: [kafka:option()]) ->
                        {ok, Pid :: pid()} |
                        {error, Reason :: any()}.
start_link(ServerName, Brokers, Options) ->
    gen_server:start_link(
      ServerName, ?MODULE, _Args = {Brokers, Options},
      _Options = []).

%% @doc Stop the client.
-spec stop(ClientRef :: kafka:client_ref()) -> ok.
stop(ClientRef) ->
    gen_server:call(ClientRef, ?STOP).

%% @doc Reconnect and reread the metadata.
-spec hup(ClientRef :: kafka:client_ref()) -> ok.
hup(ClientRef) ->
    gen_server:cast(ClientRef, ?HUP).

%% @doc Send a produce request.
-spec metadata(ClientRef :: kafka:client_ref()) ->
                      {ok, metadata_response()} |
                      {error, Reason :: any()}.
metadata(ClientRef) ->
    gen_server:call(ClientRef, ?METADATA).

%% @doc Send a produce request.
-spec produce(ClientRef :: kafka:client_ref(),
              BrokerId :: auto | broker_id(),
              RequiredAcks :: produce_request_required_acks(),
              Timeout :: produce_request_timeout(),
              Topics :: produce_request_topics()) ->
                     {ok, produce_response()} |
                     {error, produce_error_reason()}.
produce(_ClientRef, _BrokerId, ?NoAcknowledgement, _Timeout, _Topics) ->
    %% The caller tries to make synchronous produce request
    %% but the value of RequiredAcks will make it asynchronous.
    %% This definitely will cause timeout waiting for response
    %% from the leader.
    {error, ?bad_req_acks_for_sync_request};
produce(ClientRef, BrokerId, RequiredAcks, Timeout, Topics) ->
    gen_server:call(
      ClientRef, {?PRODUCE, BrokerId, RequiredAcks, Timeout, Topics}).

%% @doc Send an asynchronous produce request.
-spec produce_async(ClientRef :: kafka:client_ref(),
                    BrokerId :: auto | broker_id(),
                    Topics :: produce_request_topics()) -> ok.
produce_async(ClientRef, BrokerId, Topics) ->
    gen_server:cast(ClientRef, {?PRODUCE, BrokerId, Topics}).

%% ----------------------------------------------------------------------
%% gen_server callbacks
%% ----------------------------------------------------------------------

-record(
   state,
   {brokers = [] :: [kafka:broker()],
    options = [] :: [kafka:option()],
    sockets = [] :: [socket_item()],
    metadata :: metadata_response()
   }).

%% @hidden
-spec init(Args :: any()) -> {ok, InitialState :: #state{}}.
init({Brokers, Options}) ->
    ?trace("init(~9999p)", [{Brokers, Options}]),
    %% schedule reconnect immediately after the start but
    %% not in the init/1 fun, to not block caller process.
    ok = hup(self()),
    {ok,
     _State = #state{
       brokers = Brokers,
       options = Options
      }}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast({?PRODUCE, BrokerId, Topics}, State) ->
    ok = handle_produce_async(State, BrokerId, Topics),
    {noreply, State};
handle_cast(?HUP, State) ->
    NewState = reconnect(State),
    {noreply, NewState};
handle_cast(?RECONNECT, State) ->
    NewState = reconnect_brokers(State),
    {noreply, NewState};
handle_cast(_Request, State) ->
    ?trace("unknown cast message:~n\t~p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_info(Info :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}}.
handle_info({tcp_closed, Socket}, State) ->
    ?trace("socket ~w was closed", [Socket]),
    NewState = disconnect_broker(State, Socket),
    {noreply, NewState};
handle_info({tcp_error, Socket, _Reason}, State) ->
    ?trace("tcp_error occured for ~w: ~999p", [Socket, _Reason]),
    NewState = disconnect_broker(State, Socket),
    {noreply, NewState};
handle_info(_Request, State) ->
    ?trace("unknown info message:~n\t~p", [_Request]),
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(?METADATA, _From, State) ->
    {Reply, NewState} = handle_metadata(State),
    {reply, Reply, NewState};
handle_call({?PRODUCE, BrokerId, RequiredAcks, Timeout, Topics},
            _From, State) ->
    Reply = handle_produce(State, BrokerId, RequiredAcks, Timeout, Topics),
    {reply, Reply, State};
handle_call(?STOP, _From, State) ->
    ?trace("stopping...", []),
    {stop, _Reason = shutdown, _Reply = ok, State};
handle_call(_Request, _From, State) ->
    ?trace("unknown call from ~w:~n\t~p", [_From, _Request]),
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Reconnect and reread metadata from any broker.
-spec reconnect(OldState :: #state{}) -> NewState :: #state{}.
reconnect(OldState) ->
    ?trace("reconnecting...", []),
    _OldSeed = random:seed(now()),
    %% close all opened TCP sockets
    ok = lists:foreach(
           fun({_BrokerId, Socket}) when is_port(Socket) ->
                   ok = gen_tcp:close(Socket);
              (_SocketItem) ->
                   ok
           end, OldState#state.sockets),
    %% find first available broker
    case get_metadata_from_first_available_broker(OldState#state.brokers) of
        {ok, Metadata} ->
            ?trace("got metadata:~n\t~p", [Metadata]),
            %% open TCP connections to all discovered brokers
            SocketItems =
                [{BrokerId, _Socket = undefined} ||
                    {BrokerId, _Host, _Port} <-
                        Metadata#metadata_response.brokers],
            reconnect_brokers(
              OldState#state{
                sockets = SocketItems,
                metadata = Metadata
               });
        error ->
            %% no alive brokers found. Reschedule connection later
            ok = schedule_reconnect(),
            OldState#state{
              sockets = [],
              metadata = undefined
             }
    end.

%% @doc Reschedule reconnect later.
-spec schedule_reconnect() -> ok.
schedule_reconnect() ->
    ?trace("total reconnect scheduled", []),
    {ok, _TRef} =
        timer:apply_after(?CONNECT_RETRY_PERIOD, ?MODULE, hup, []),
    ok.

%% @doc Try to read metadata from any available broker.
-spec get_metadata_from_first_available_broker(
        Brokers :: [kafka:broker()]) -> {ok, metadata_response()} | error.
get_metadata_from_first_available_broker([]) ->
    error;
get_metadata_from_first_available_broker([{Address, PortNumber} | Tail]) ->
    %% Important note:
    %% {packet,4} option implies that the packet header is unsigned
    %% big endian, but Kafka protocol encodes the packet length as
    %% int32, which is SIGNED big endian.
    TcpOpts = [binary, {packet, 4}, {reuseaddr, true}, {active, true}],
    ?trace("bootstrap: connecting to ~999p:~w...", [Address, PortNumber]),
    case gen_tcp:connect(Address, PortNumber, TcpOpts, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            ?trace(
               "bootstrap: asking metadata from ~999p:~w...",
               [Address, PortNumber]),
            case get_metadata(Socket) of
                {ok, _Metadata} = Ok ->
                    ok = gen_tcp:close(Socket),
                    Ok;
                {error, _Reason} ->
                    ok = gen_tcp:close(Socket),
                    get_metadata_from_first_available_broker(Tail)
            end;
        {error, _Reason} ->
            ?trace(
               "bootstrap: failed to connect to ~999p:~w: ~999p",
               [Address, PortNumber, _Reason]),
            get_metadata_from_first_available_broker(Tail)
    end.

%% @doc Ask metadata from a broker.
-spec get_metadata(Socket :: port()) ->
                          {ok, metadata_response()} |
                          {error, Reason :: any()}.
get_metadata(Socket) ->
    do_sync_request(
      Socket,
      _Request = #request{
        api_key = ?MetadataRequest,
        message = [] %% ask for metadata for all topics
       }).

%% @doc Generate a new value for CorrelationId field.
-spec gen_corellation_id() -> corellation_id().
gen_corellation_id() ->
    random:uniform(16#ffffffff + 1) - 2147483648 - 1.

%% @doc Reconnect disconnected brokers.
%% Will try to connect each broker when second element of
%% socket_item (in #state.sockets list) is not an open port.
%% If at least one broker remains unconnected, the function
%% will schedule next reconnect attempt later.
-spec reconnect_brokers(OldState :: #state{}) ->
                               NewState :: #state{}.
reconnect_brokers(OldState) ->
    ?trace("reconnecting brokers...", []),
    Brokers = (OldState#state.metadata)#metadata_response.brokers,
    NewSockets =
        lists:map(
          fun({_BrokerId, Socket} = SocketItem) when is_port(Socket) ->
                  %% already connected
                  SocketItem;
             ({BrokerId, _NotASocket}) ->
                  %% need to reconnect
                  {BrokerId, Host, Port} =
                      lists:keyfind(BrokerId, 1, Brokers),
                  {BrokerId, connect_broker(Host, Port)}
          end, OldState#state.sockets),
    case is_all_brokers_connected(NewSockets) of
        true ->
            ?trace("all brokers are connected", []);
        false ->
            ok = schedule_broker_reconnect()
    end,
    OldState#state{sockets = NewSockets}.

%% @doc Schedule broker reconnection task.
-spec schedule_broker_reconnect() -> ok.
schedule_broker_reconnect() ->
    ?trace("brokers reconnect scheduled", []),
    {ok, _TRef} =
        timer:apply_after(
          ?BROKER_CONNECT_RETRY_PERIOD,
          gen_server, cast, [self(), ?RECONNECT]),
    ok.

%% @doc Try to establish a TCP connection to a broker.
-spec connect_broker(Host :: string(), Port :: inet:port_number()) ->
                            Socket :: port() | undefined.
connect_broker(Host, Port) ->
    %% Important note:
    %% {packet,4} option implies that the packet header is unsigned
    %% big endian, but Kafka protocol encodes the packet length as
    %% int32, which is SIGNED big endian.
    TcpOpts = [binary, {packet, 4}, {active, true}],
    ?trace("connecting to ~s:~w...", [Host, Port]),
    case gen_tcp:connect(Host, Port, TcpOpts, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            ?trace("connected to ~s:~w", [Host, Port]),
            Socket;
        {error, _Reason} ->
            ?trace(
               "failed to connect to ~s:~w: ~999p",
               [Host, Port, _Reason]),
            undefined
    end.

%% @doc Return false if there is at least one not connected broker.
-spec is_all_brokers_connected(SocketItems :: [socket_item()]) ->
                                      boolean().
is_all_brokers_connected(List) ->
    lists:all(
      fun({_BrokerId, Socket}) when is_port(Socket) ->
              true;
         (_) ->
              false
      end, List).

%% @doc Close selected socket, schedule broker reconnection
%% and update process internal state.
-spec disconnect_broker(OldState :: #state{},
                        SocketToClose :: port()) ->
                               NewState :: #state{}.
disconnect_broker(OldState, SocketToClose) ->
    NewSockets =
        lists:map(
          fun({BrokerId, Socket}) when Socket == SocketToClose ->
                  ?trace("broker #~s disconnected", [BrokerId]),
                  ok = gen_tcp:close(Socket),
                  {BrokerId, undefined};
             (SocketItem) ->
                  SocketItem
          end, OldState#state.sockets),
    ok = schedule_broker_reconnect(),
    OldState#state{sockets = NewSockets}.

%% @doc Do metadata request to the Kafka cluster.
-spec handle_metadata(OldState :: #state{}) ->
                             {Reply :: ({ok, metadata_response()} |
                                        {error, Reason :: any()}),
                              NewState :: #state{}}.
handle_metadata(OldState) ->
    %% take first connected broker
    ActiveSockets =
        [Socket ||
            {_BrokerId, Socket} <- OldState#state.sockets,
            is_port(Socket)],
    case ActiveSockets of
        [Socket | _] ->
            case get_metadata(Socket) of
                {ok, _Metadata} = Ok ->
                    {Ok, OldState};
                {error, _Reason} ->
                    %% failover to next connected broker
                    NewState = disconnect_broker(OldState, Socket),
                    handle_metadata(NewState)
            end;
        [] ->
            %% no active connections present
            {_Reply = {error, ?not_connected}, OldState}
    end.

%% @doc Do produce request to the Kafka cluster.
-spec handle_produce(State :: #state{},
                     BrokerId :: auto | broker_id(),
                     RequiredAcks :: produce_request_required_acks(),
                     Timeout :: produce_request_timeout(),
                     Topics :: produce_request_topics()) ->
                            {ok, Reply :: produce_response()} |
                            {error,
                             leader_search_error_reason() |
                             ?not_connected |
                             do_sync_request_error_reason()}.
handle_produce(State, BrokerId, RequiredAcks, Timeout, Topics) ->
    ?trace("handle_produce(): topics:~n\t~p", [Topics]),
    case resolve_leader(BrokerId, State#state.metadata, Topics) of
        {ok, LeaderId} ->
            case lookup_socket_by_broker_id(
                   State#state.sockets, LeaderId) of
                {ok, Socket} ->
                    ProduceRequest = #produce_request{
                      required_acks = RequiredAcks,
                      timeout = Timeout,
                      topics = Topics
                     },
                    Request = #request{
                      api_key = ?ProduceRequest,
                      message = ProduceRequest
                     },
                    do_sync_request(Socket, Request);
                undefined ->
                    ?trace("broker #~w is not connected", [LeaderId]),
                    {error, ?not_connected}
            end;
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Do produce request to the Kafka cluster.
-spec handle_produce_async(State :: #state{},
                           BrokerId :: auto | broker_id(),
                           Topics :: produce_request_topics()) -> ok.
handle_produce_async(State, BrokerId, Topics) ->
    ?trace("handle_produce_async(): topics:~n\t~p", [Topics]),
    case resolve_leader(BrokerId, State#state.metadata, Topics) of
        {ok, LeaderId} ->
            case lookup_socket_by_broker_id(
                   State#state.sockets, LeaderId) of
                {ok, Socket} ->
                    ProduceRequest = #produce_request{
                      required_acks = ?NoAcknowledgement,
                      timeout = 0,
                      topics = Topics
                     },
                    Request = #request{
                      api_key = ?ProduceRequest,
                      message = ProduceRequest
                     },
                    do_async_request(Socket, Request);
                undefined ->
                    ?trace("broker #~w is not connected", [LeaderId])
            end;
        {error, _Reason} ->
            ok
    end.

%% @doc Lookup TCP socket by the broker ID.
-spec lookup_socket_by_broker_id(Sockets :: [socket_item()],
                                 BrokerId :: broker_id()) ->
                                        {ok, Socket :: port()} |
                                        undefined.
lookup_socket_by_broker_id(Sockets, BrokerId) ->
    case lists:keyfind(BrokerId, 1, Sockets) of
        {BrokerId, Socket} when is_port(Socket) ->
            {ok, Socket};
        _ ->
            undefined
    end.

%% @doc Resolve broker ID (a leader) for the request to send if needed.
-spec resolve_leader(RequestedBrokerId :: auto | broker_id(),
                     Metadata :: metadata_response(),
                     Topics :: produce_request_topics()) ->
         {ok, BrokerId :: broker_id()} |
         {error, leader_search_error_reason()}.
resolve_leader(BrokerId, _Metadata, _Topics) when is_integer(BrokerId) ->
    ?trace("the leader is set to: ~w", [BrokerId]),
    {ok, BrokerId};
resolve_leader(auto, Metadata, Topics) ->
    ?trace("the leader is not set. Looking for any partition...", []),
    case get_first_partition(Topics) of
        {TopicName, PartitionId} ->
            ?trace(
               "partition #~w found in topic ~9999p",
               [PartitionId, TopicName]),
            get_partition_leader(Metadata, TopicName, PartitionId);
        undefined ->
            ?trace("no partition was found (empty request?)", []),
            {error, ?no_leader_for_empty_request}
    end.

%% @doc Get first topic-partition pair from the produce request.
-spec get_first_partition(Topics :: produce_request_topics()) ->
                                 {topic_name(), partition_id()} |
                                 undefined.
get_first_partition(Topics) ->
    %% Notice:
    %% It can be useful in future to filter out partitions
    %% with empty message set as there is nothing to produce
    %% to the partition.
    Partitions =
        [{TopicName, Partition} ||
            {TopicName, Partitions} <- Topics,
            {Partition, _MessageSet} <- Partitions],
    case Partitions of
        [{_TopicName, _Partition} = Item | _Tail] ->
            Item;
        [] ->
            undefined
    end.

%% @doc Find a leader for a given topic and partition.
-spec get_partition_leader(Metadata :: metadata_response(),
                           TopicName :: topic_name(),
                           PartitionId :: partition_id()) ->
         {ok, BrokerId :: broker_id()} |
         {error, partition_leader_search_error_reason()}.
get_partition_leader(Metadata, TopicName, PartitionId) ->
    ?trace(
       "looking for a leader for ~9999p:~w...",
       [TopicName, PartitionId]),
    case lists:keyfind(TopicName, 2, Metadata#metadata_response.topics) of
        {?NoError, TopicName, Partitions} ->
            case lists:keyfind(PartitionId, 2, Partitions) of
                {?NoError, PartitionId, -1, _Replicas, _Isr} ->
                    ?trace("leader election is in progress", []),
                    {error, ?leader_election(TopicName, PartitionId)};
                {?NoError, PartitionId, Leader, _Replicas, _Isr} ->
                    ?trace("the leader is: ~w", [Leader]),
                    {ok, Leader};
                {ErrorCode, PartitionId, _Leader, _Replicas, _Isr} ->
                    ?trace("partition is not ok. ErrorCode=~w", [ErrorCode]),
                    {error,
                     ?partition_error(TopicName, PartitionId, ErrorCode)};
                false ->
                    ?trace("no such partition", []),
                    {error, ?no_such_partition(TopicName, PartitionId)}
            end;
        {ErrorCode, TopicName, _Partitions} ->
            ?trace("topic is not ok. ErrorCode=~w", [ErrorCode]),
            {error, ?topic_error(TopicName, ErrorCode)};
        false ->
            ?trace("no such topic", []),
            {error, ?no_such_topic(TopicName)}
    end.

%% @doc Do a synchronous request, wait for a response, receive
%% it, decode and return as function result.
-spec do_sync_request(Socket :: port(), Request :: request()) ->
                             {ok, response()} |
                             {error, do_sync_request_error_reason()}.
do_sync_request(Socket, Request) ->
    CorellationId = gen_corellation_id(),
    ApiKey = Request#request.api_key,
    %% encode the request
    EncodedRequest =
        kafka_proto:encode(
          Request#request{
            corellation_id = CorellationId,
            client_id = "erlang-kafka-client"
           }),
    %% send the request and wait for a response
    ?trace("sending to ~w 0x~4.16.0B bytes with CorrelationId=~w...",
           [element(2, inet:peername(Socket)),
            size(EncodedRequest), CorellationId]),
    case gen_tcp:send(Socket, EncodedRequest) of
        ok ->
            receive
                {tcp, Socket, EncodedResponse} ->
                    %% decode the response
                    Response = kafka_proto:decode(ApiKey, EncodedResponse),
                    %% check the corellation id
                    ResponseCorellationId =
                        Response#response.corellation_id,
                    if ResponseCorellationId == CorellationId ->
                            {ok, Response#response.message};
                       true ->
                            ?trace(
                               "corellation ids are not match: ~w /= ~w",
                               [CorellationId, ResponseCorellationId]),
                            {error, ?corellation_id_mismatch}
                    end;
                {tcp_closed, Socket} ->
                    ?trace("failed to receive the response: tcp_closed", []),
                    {error, ?tcp_closed};
                {tcp_error, Socket, Reason} ->
                    ?trace(
                       "failed to receive the response: ~999p",
                       [Reason]),
                    {error, Reason}
            after ?RESPONSE_READ_TIMEOUT ->
                    ?trace("failed to receive the response: timeout", []),
                    {error, ?timeout}
            end;
        {error, Reason} ->
            ?trace("failed to send request: ~999p", [Reason]),
            {error, Reason}
    end.

%% @doc Do an asynchronous request.
-spec do_async_request(Socket :: port(), Request :: request()) -> ok.
do_async_request(Socket, Request) ->
    CorellationId = gen_corellation_id(),
    %% encode the request
    EncodedRequest =
        kafka_proto:encode(
          Request#request{
            corellation_id = CorellationId,
            client_id = "erlang-kafka-client"
           }),
    %% send the request and wait for a response
    ?trace("sending to ~w 0x~4.16.0B bytes with CorrelationId=~w...",
           [element(2, inet:peername(Socket)),
            size(EncodedRequest), CorellationId]),
    case gen_tcp:send(Socket, EncodedRequest) of
        ok ->
            ok;
        {error, _Reason} ->
            ?trace("failed to send request: ~999p", [_Reason])
    end.
