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
    stop/1,
    hup/1
   ]).

%% gen_server callback exports
-export([init/1, handle_call/3, handle_info/2, handle_cast/2,
         terminate/2, code_change/3]).

%% testing exports
-export(
   [get_metadata_from_first_available_broker/1,
    get_metadata/1
   ]).

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

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-type socket_item() ::
        {BrokerId :: kafka_proto:broker_id(),
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
      {local, ?MODULE}, ?MODULE, _Args = {Brokers, Options},
      _Options = []).

%% @doc Stop the client.
-spec stop(Pid :: pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, ?STOP).

%% @doc Reconnect and reread the metadata.
-spec hup(Pid :: pid()) -> ok.
hup(Pid) ->
    gen_server:cast(Pid, ?HUP).

%% ----------------------------------------------------------------------
%% gen_server callbacks
%% ----------------------------------------------------------------------

-record(
   state,
   {brokers = [] :: [kafka:broker()],
    options = [] :: [kafka:option()],
    sockets = [] :: [socket_item()],
    metadata :: #metadata_response{}
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
    ok = lists:foreach(fun gen_tcp:close/1, OldState#state.sockets),
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
        Brokers :: [kafka:broker()]) -> {ok, #metadata_response{}} | error.
get_metadata_from_first_available_broker([]) ->
    error;
get_metadata_from_first_available_broker([{Address, PortNumber} | Tail]) ->
    %% Important note:
    %% {packet,4} option implies that the packet header is unsigned
    %% big endian, but Kafka protocol encodes the packet length as
    %% int32, which is SIGNED big endian.
    TcpOpts = [binary, {packet, 4}, {reuseaddr, true}, {active, false}],
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
                error ->
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
                          {ok, #metadata_response{}} | error.
get_metadata(Socket) ->
    %% form the request
    CorellationId = gen_corellation_id(),
    MetadataRequest = [], %% ask for metadata for all topics
    Request = #request{
      api_key = ?MetadataRequest,
      corellation_id = CorellationId,
      client_id = "erlang-kafka-client",
      message = MetadataRequest
     },
    %% encode the request
    EncodedRequest = kafka_proto:encode(Request),
    %% send the request and wait for a response
    ?trace("sending 0x~4.16.0B bytes with CorrId=~w...",
           [size(EncodedRequest), CorellationId]),
    case gen_tcp:send(Socket, EncodedRequest) of
        ok ->
            case gen_tcp:recv(
                   Socket, _Length = 0, ?RESPONSE_READ_TIMEOUT) of
                {ok, EncodedResponse} ->
                    %% decode the response
                    Response = kafka_proto:decode(EncodedResponse),
                    %% check the corellation id
                    ResponseCorellationId =
                        Response#response.corellation_id,
                    if ResponseCorellationId == CorellationId ->
                            {ok, Response#response.message};
                       true ->
                            ?trace(
                               "corellation ids are not match: ~w /= ~w",
                               [CorellationId, ResponseCorellationId]),
                            error
                    end;
                {error, _Reason} ->
                    ?trace(
                       "failed to receive the response: ~999p",
                       [_Reason]),
                    error
            end;
        {error, _Reason} ->
            ?trace("failed to send request: ~999p", [_Reason]),
            error
    end.

%% @doc Generate a new value for CorrelationId field.
-spec gen_corellation_id() -> kafka_proto:int32().
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
