%%
%% epgsql request module
%%
%%

-module(eqpg_epgsql).

-behaviour(gen_server).


-export([start/2, stop/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

% api
%-export([call/3, cast/3, cast/4]).

% func
%-export([query/2]).

-include("../include/eqpg.hrl").


start(PoolName, Args) -> gen_server:start(?MODULE, [PoolName, Args], []).
% 
init([PoolName, Args = #{host := Host, user := User, pass := Pass, base := Db}]) ->
  Opts = [{database, Db}, {timeout, maps:get(timeout, Args, 4000)}],
  case epgsql:connect(Host, User, Pass, Opts) of
    {ok, Conn} -> S = #{pool => PoolName, conn => Conn, status => idle}, link(Conn), {ok, S};
    Else       -> {stop, ?e(init_pg_conn_error, Else)}
  end.

stop(Pid) ->
  gen_server:stop(Pid).


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% OTP gen_server api
handle_info(timeout, State) -> timeout_(State);
handle_info(Message, State) -> info_unknown(State, Message).
code_change(_Old, State, _) -> {ok, State}.
terminate(_Reason, _State)  -> ok.

%%casts
handle_cast({query, F, Req}, S) -> query_(S, F, Req); 
handle_cast(Msg, State)         -> cast_unknown(State, Msg).
%%calls
handle_call(Msg, _From, State)  -> call_unknown(State, Msg).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%




% query(Pid, From, Req) -> gen_server:cast(Pid, {query, FromPid, Req}).
query_(S = #{conn := C}, FromPid, {Ref, Sql, _Timeout}) ->
  Answer = epgsql:squery(C, binary_to_list(Sql)),
  timer:sleep(50),
  gen_server:cast(FromPid, {answer, {Ref, Answer}}),
  {noreply, S#{status := buzy}, 0}.



%
timeout_(S = #{status := buzy, pool := Pool}) -> 
  %% TODO erespool reg pool name
  ers:ret_conn(Pool, self()), 
  {noreply, S#{status := idle}};
%
timeout_(S = #{status := idle}) -> 
  {noreply, S}.




cast_unknown(State, Msg) -> 
  ?INF("cast_unknown", Msg),
  {noreply, State, 0}.

info_unknown(State, Msg) -> 
  ?INF("info_unknown", Msg),
  {noreply, State, 0}.

call_unknown(State, Msg) ->
  ?INF("call_unknown", Msg),
  {reply, ?e(unknown_call), State, 0}.



      


