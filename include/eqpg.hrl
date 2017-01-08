

%% IF
-define(IF(Cond, TrueVal, FalseVal), case Cond of true -> TrueVal; false -> FalseVal end).

%% Point
-define(p,         list_to_binary(io_lib:format("Mod:~w line:~w",       [?MODULE,?LINE]))).
-define(p(Reason), list_to_binary(io_lib:format("Mod:~w line:~w ~100P", [?MODULE,?LINE, Reason, 300]))).

%% Error
-type err() :: {err, {atom(), binary()}}.
-define(e(ErrCode), {err, {ErrCode, ?p}}).
-define(e(ErrCode, Reason), {err, {ErrCode, ?p(Reason)}}).


% NOW time in seconds
-define(second, erlang:system_time(second)).
% NOW time in milliseconds
-define(millisecond, erlang:system_time(millisecond)).


%% Log messages
-define(INF(Str, Term), io:format("EQPG INFO: ~p:~p ~p ~100P~n", [?MODULE, ?LINE, Str, Term, 300])).
%-define(INF(Str, Term), lager:info(   "~p:~p ~p ~100P~n", [?MODULE, ?LINE, Str, lager:pr(Term, ?MODULE), 300])).

