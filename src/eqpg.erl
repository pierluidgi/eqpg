%%
%%
%%
%%

-module(eqpg).

-include("../include/eqpg.hrl").

-export([create/2, delete/0, delete/1]).
-export([query/2, query/3]).
-export([list/0, stat/1, info/1]).


-spec create(Q::map(), Pool::map()) -> {ok, Pid::pid()}|err().
create(_Queue = #{name := QName}, _Pool = #{name := PoolName, args := PoolArgs}) ->
  %% TODO _Queue = #{name := QName, args := QArgs}, QArgs options to custom eqpg_queue module work
  M = eqpg_queue,
  F = start_link,
  A = #{name => QName, pool_name => PoolName, pool_args => PoolArgs},
  case eqpg_sup:start_queue(#{name => QName, mfa => {M, F, [A]}}) of
    {ok, Pid} -> {ok, Pid};
    {error, {already_started, Pid}} -> {err, {already_started, Pid}};
    {error, {Code, Err}} when is_atom(Code) -> ?e(Code, Err);
    Else -> ?e(unknown_error, Else)
  end.



delete() -> list().
-spec delete(Q::pid()|atom()) -> ok|err().
delete(Q) -> eqpg_sup:stop_queue(Q).



%%
query(Q, Sql) ->
  query(Q, Sql, 2000).
%%
query(Q, Sql, Timeout) ->
  gen_server:call(Q, {query, {Sql, Timeout}}).




%% MISC 
-spec list() -> {ok, list()}|err().
list() -> 
  {ok, [{Name, Pid} || {Name, Pid, _, _} <- supervisor:which_children(eqpg_sup)]}.
  

-spec info(Q::pid()|atom()) -> {ok, map()}|err().
info(_Q) -> {ok, #{}}. %% TODO


-spec stat(Q::pid()|atom()) -> {ok, map()}|err().
stat(Q) -> gen_server:call(Q, stat).

