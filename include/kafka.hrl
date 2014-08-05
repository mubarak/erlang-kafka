%%%----------------------------------------------------------------------
%%% File        : kafka.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@proffero.com>
%%% Description : kafka definitions file
%%% Created     : 04 Aug 2014
%%%----------------------------------------------------------------------

-ifndef(_KAFKA).
-define(_KAFKA, true).

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
