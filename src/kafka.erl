%%% @doc
%%% Erlang Client for the Apache Kafka 0.8.x.
%%%
%%% See
%%% [https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol]
%%% for protocol implementation details.

%%% @author Aleksey Morarash <aleksey.morarash@proffero.com>
%%% @since 04 Aug 2014
%%% @copyright 2014, Proffero <info@proffero.com>

-module(kafka).

%% API exports
-export(
   [start_link/2,
    start_link/3,
    hup/1,
    close/1,
    metadata/1,
    produce/5,
    produce_async/3
   ]).

-include("kafka.hrl").
-include("kafka_proto.hrl").

%% --------------------------------------------------------------------
%% Type definitions
%% --------------------------------------------------------------------

-export_type(
   [broker/0,
    broker_address/0,
    option/0,
    error_reason/0,
    server_name/0,
    client_ref/0
   ]).

-type broker() :: {Address :: broker_address(),
                   PortNumber :: inet:port_number()}.

-type broker_address() ::
        nonempty_string() | inet:ip_address().

-type option() ::
        {topics, [topic_name()]}.

-type server_name() ::
        {local, Name :: atom()} |
        {global, GlobalName :: any()} |
        {via, Module :: atom(), ViaName :: any()}.

-type client_ref() ::
        (Name :: atom()) |
        {Name :: atom(), Node :: node()} |
        {global, GlobalName :: any()} |
        {via, Module :: atom(), ViaName :: any()} |
        pid().

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start an Kafka client in a linked process.
-spec start_link(Brokers :: [broker()],
                 Options :: [option()]) ->
                        {ok, Pid :: pid()} |
                        {error, Reason :: error_reason()}.
start_link(Brokers, Options) ->
    case check_start_args(Brokers, Options) of
        ok ->
            kafka_client:start_link(Brokers, Options);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Start an named Kafka client in a linked process.
%% Purpose of ServerName argument you can find at
%% the gen_server:start_link/4 function description.
-spec start_link(ServerName :: server_name(),
                 Brokers :: [broker()],
                 Options :: [option()]) ->
                        {ok, Pid :: pid()} |
                        {error, Reason :: error_reason()}.
start_link(ServerName, Brokers, Options) ->
    case check_start_args(Brokers, Options) of
        ok ->
            kafka_client:start_link(ServerName, Brokers, Options);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Reconnect and reread the metadata.
-spec hup(ClientRef :: client_ref()) -> ok.
hup(ClientRef) ->
    kafka_client:hup(ClientRef).

%% @doc Stop the client.
-spec close(ClientRef :: client_ref()) -> ok.
close(ClientRef) ->
    kafka_client:stop(ClientRef).

%% @doc Send a produce request.
-spec metadata(ClientRef :: kafka:client_ref()) ->
                      {ok, metadata_response()} |
                      {error, Reason :: any()}.
metadata(ClientRef) ->
    kafka_client:metadata(ClientRef).

%% @doc Send a produce request.
-spec produce(ClientRef :: kafka:client_ref(),
              BrokerId :: auto | broker_id(),
              RequiredAcks :: produce_request_required_acks(),
              Timeout :: produce_request_timeout(),
              Topics :: produce_request_topics()) ->
                     {ok, produce_response()} |
                     {error, produce_error_reason()}.
produce(ClientRef, BrokerId, RequiredAcks, Timeout, Topics) ->
    kafka_client:produce(ClientRef, BrokerId, RequiredAcks, Timeout, Topics).

%% @doc Send an asynchronous produce request.
-spec produce_async(ClientRef :: kafka:client_ref(),
                    BrokerId :: auto | broker_id(),
                    Topics :: produce_request_topics()) -> ok.
produce_async(ClientRef, BrokerId, Topics) ->
    kafka_client:produce_async(ClientRef, BrokerId, Topics).

%% --------------------------------------------------------------------
%% Internal functions
%% --------------------------------------------------------------------

%% @doc Check start args.
-spec check_start_args(Broker :: [broker()],
                       Options :: [option()]) ->
                              ok |
                              {error,
                               ?bad_broker(any()) |
                               ?bad_brokers |
                               ?bad_option(any()) |
                               ?bad_options}.
check_start_args([], _Options) ->
    {error, ?bad_brokers};
check_start_args(Brokers, Options) ->
    case check_brokers(Brokers) of
        ok ->
            check_options(Options);
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Check the broker list.
-spec check_brokers(Brokers :: [broker()]) ->
                           ok |
                           {error, ?bad_broker(any())} |
                           {error, ?bad_brokers}.
check_brokers([]) ->
    ok;
check_brokers([Broker | Tail]) ->
    case check_broker(Broker) of
        ok ->
            check_brokers(Tail);
        error ->
            {error, ?bad_broker(Broker)}
    end;
check_brokers(_BadBrokers) ->
    {error, ?bad_brokers}.

%% @doc Check broker address and port.
-spec check_broker(Broker :: broker()) -> ok | error.
check_broker({{A, B, C, D}, P})
  when ?is_uint8(A) andalso ?is_uint8(B) andalso
       ?is_uint8(C) andalso ?is_uint8(D) andalso
       ?is_port_number(P) ->
    ok;
check_broker({{A, B, C, D, E, F, G, H}, P})
  when ?is_uint16(A) andalso ?is_uint16(B) andalso
       ?is_uint16(C) andalso ?is_uint16(D) andalso
       ?is_uint16(E) andalso ?is_uint16(F) andalso
       ?is_uint16(G) andalso ?is_uint16(H) andalso
       ?is_port_number(P) ->
    ok;
check_broker({[_ | _] = _String, P}) when ?is_port_number(P) ->
    ok;
check_broker(_BadBroker) ->
    error.

%% @doc Check the options.
-spec check_options(Options :: [option()]) ->
                           ok |
                           {error, ?bad_option(any())} |
                           {error, ?bad_options}.
check_options([]) ->
    ok;
check_options([Option | Tail]) ->
    case check_option(Option) of
        ok ->
            check_options(Tail);
        error ->
            {error, ?bad_option(Option)}
    end;
check_options(_BadOptions) ->
    {error, ?bad_options}.

%% @doc Check the option.
-spec check_option(Option :: option()) -> ok | error.
check_option({topics, List}) when is_list(List) ->
    true =
        lists:all(
          fun([_ | _] = _NonEmptyString) ->
                  true;
             (_) ->
                  false
          end, List),
    ok;
check_option(_BadOption) ->
    error.
