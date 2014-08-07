%%%----------------------------------------------------------------------
%%% File        : kafka.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@proffero.com>
%%% Description : kafka definitions file
%%% Created     : 04 Aug 2014
%%%----------------------------------------------------------------------

-ifndef(_KAFKA).
-define(_KAFKA, true).

-define(CONNECT_TIMEOUT, 2000). %% two seconds
-define(CONNECT_RETRY_PERIOD, 5000). %% five seconds
-define(BROKER_CONNECT_RETRY_PERIOD, 5000). %% five seconds
-define(RESPONSE_READ_TIMEOUT, 10000). %% ten seconds

%% ----------------------------------------------------------------------
%% error reasons

-define(bad_brokers, bad_brokers).
-define(bad_broker(Term), {bad_broker, Term}).
-define(bad_options, bad_options).
-define(bad_option(Term), {bad_option, Term}).
-define(not_connected, not_connected).
-define(corellation_id_mismatch, corellation_id_mismatch).
-define(topic_error(TopicName, ErrorCode),
        {topic_error, TopicName, ErrorCode}).
-define(no_such_topic(TopicName),
        {no_such_topic, TopicName}).
-define(leader_election(TopicName, PartitionId),
        {leader_election, TopicName, PartitionId}).
-define(partition_error(TopicName, PartitionId, ErrorCode),
        {partition_error, TopicName, PartitionId, ErrorCode}).
-define(no_such_partition(TopicName, PartitionId),
        {no_such_partition, TopicName, PartitionId}).
-define(no_leader_for_empty_request, no_leader_for_empty_request).
-define(tcp_closed, tcp_closed).
-define(timeout, timeout).
-define(bad_req_acks_for_sync_request, bad_req_acks_for_sync_request).

-type error_reason() ::
        start_error_reason() |
        ?not_connected |
        produce_error_reason().

-type start_error_reason() ::
        ?bad_brokers |
        ?bad_broker(any()) |
        ?bad_options |
        ?bad_option(any()).

-type produce_error_reason() ::
        leader_search_error_reason() |
        ?not_connected |
        ?corellation_id_mismatch |
        ?bad_req_acks_for_sync_request |
        do_sync_request_error_reason().

-type leader_search_error_reason() ::
        ?no_leader_for_empty_request |
        partition_leader_search_error_reason().

-type partition_leader_search_error_reason() ::
        ?topic_error(topic_name(), error_code()) |
        ?no_such_topic(topic_name()) |
        ?leader_election(topic_name(), partition_id()) |
        ?partition_error(topic_name(), partition_id(), error_code()) |
        ?no_such_partition(topic_name(), partition_id()).

-type do_sync_request_error_reason() ::
        ?corellation_id_mismatch |
        ?tcp_closed |
        ?timeout |
        closed | inet:posix().

%% ----------------------------------------------------------------------
%% guards

-define(is_uint8(I),
        (is_integer(I) andalso I >= 0 andalso I =< 16#ff)).

-define(is_uint16(I),
        (is_integer(I) andalso I >= 0 andalso I =< 16#ffff)).

-define(is_port_number(I),
        (is_integer(I) andalso I > 0 andalso I =< 16#ffff)).

%% ----------------------------------------------------------------------
%% debugging

-ifdef(TRACE).
-define(
   trace(Format, Args),
   ok = io:format(
          "TRACE> mod:~w; line:~w; pid:~w; msg:" ++ Format ++ "~n",
          [?MODULE, ?LINE, self() | Args]
         )).
-else.
-define(trace(F, A), ok).
-endif.

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
