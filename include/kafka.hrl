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
-define(RESPONSE_READ_TIMEOUT, 10000). %% ten seconds

%% ----------------------------------------------------------------------
%% error reasons

-define(bad_brokers, bad_brokers).
-define(bad_broker, bad_broker).
-define(bad_options, bad_options).
-define(bad_option, bad_option).

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
          "TRACE> pid:~w; msg:" ++ Format ++ "~n", [self() | Args]
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
