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
    sockets = [] :: [port()],
    metadata :: #metadata_response{}
   }).

%% @hidden
-spec init(Args :: any()) -> {ok, InitialState :: #state{}}.
init({Brokers, Options}) ->
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
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(Info :: any(), State :: #state{}) ->
                         {noreply, State :: #state{}}.
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(?STOP, _From, State) ->
    {stop, _Reason = shutdown, _Reply = ok, State};
handle_call(_Request, _From, State) ->
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
    _OldSeed = random:seed(now()),
    %% close all opened TCP sockets
    ok = lists:foreach(fun gen_tcp:close/1, OldState#state.sockets),
    %% find first available broker
    case get_metadata_from_first_available_broker(OldState#state.brokers) of
        {ok, Metadata} ->
            %% open TCP connections to all discovered brokers
            TcpOpts = [binary, {packet, 4}, {active, true}],
            Sockets =
                [Socket ||
                    {ok, Socket} <-
                        [gen_tcp:connect(
                           Host, Port, TcpOpts, ?CONNECT_TIMEOUT) ||
                            {_NodeId, Host, Port} <-
                                Metadata#metadata_response.brokers]],
            OldState#state{
              sockets = Sockets,
              metadata = Metadata
             };
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
    case gen_tcp:connect(Address, PortNumber, TcpOpts, ?CONNECT_TIMEOUT) of
        {ok, Socket} ->
            case get_metadata(Socket) of
                {ok, _Metadata} = Ok ->
                    ok = gen_tcp:close(Socket),
                    Ok;
                error ->
                    ok = gen_tcp:close(Socket),
                    get_metadata_from_first_available_broker(Tail)
            end;
        {error, _Reason} ->
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
