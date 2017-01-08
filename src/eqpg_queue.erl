%%
%% main nonblocking queue module
%%
%%

-module(eqpg_queue).

-behaviour(gen_server).


-export([start_link/1]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).


-include("../include/eqpg.hrl").


%%
start_link(Args = #{name := Name}) -> 
  gen_server:start_link({local, Name}, ?MODULE, Args, []).
%%
init(#{pool_name := PoolName, pool_args := PoolArgs}) ->
  case ers:start(PoolName, PoolArgs) of
    {ok, Pid} -> 
      process_flag(trap_exit, true),
      {ok, #{pool          => Pid,
             in            => [],              %% In queue
             until         => ordsets:new(),   %% Until times ord set
             w_seq         => 0,
             in_count      => 0,
             ok_count      => 0,
             timeout_count => 0,
             in_rate       => mavg:new(),      %%
             ok_rate       => mavg:new(),      %%
             %er_rate       => mavg:new(),      %%
             workers_rate  => mavg:new(),      %%
             timeout_rate  => mavg:new()}      %% Until queue
      };
    Else -> 
      {stop, ?e(start_pool_error, Else)}
  end.

%%
terminate(_Reason, _State = #{pool := Pool}) -> 
  ers:stop(Pool).



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% OTP gen_server api
code_change(_OldV, State, _Extra) -> {ok, State}.

%%
handle_info(timeout, State)       -> timeout_(State);
handle_info(Msg, State)           -> info_unknown(State, Msg).

%% casts
handle_cast({worker, W, L, S}, State)-> worker_(State, W, L, S);
handle_cast({answer, A}, State)   -> answer_(State, A);
handle_cast(Msg, State)           -> cast_unknown(State, Msg).
%% calls
handle_call({query, Q}, F, State) -> query_(State, F, Q);
handle_call(stat, _F, State)      -> stat_(State);
handle_call(Msg, _F, State)       -> call_unknown(State, Msg).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% timeout_(S = #{in := In, until := Un}) {{{
timeout_(S = #{in := In, until := Un, timeout_count := TC, timeout_rate := TR}) ->
  Now = ?millisecond,

  TailFun = fun
    (Fu, NewIn, NewUn = [{Until, Ref, {req, From}}|RestUn], N) ->
      case Until =< Now of
        true -> %% Send timeout answer
          gen_server:reply(From, ?e(timeout)),
          Fu(Fu, lists:keydelete(Ref, 1, NewIn), RestUn, N+1);
        false -> 
          {NewIn, NewUn, Until - Now, N}
      end;
    (_F, NewIn, [], N) -> {[], NewIn, 100000, N}
  end,
  
  {NewIn, NewUn, NewQTimeout, TimeoutsNum} = TailFun(TailFun, In, lists:reverse(Un), 0),

  NewS = S#{in            := NewIn, 
            until         := NewUn, 
            timeout_count := TC + TimeoutsNum,
            timeout_rate  := mavg:event(TR, TimeoutsNum)},
  {noreply, NewS, NewQTimeout}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Unknown messages {{{
cast_unknown(State, Msg) ->
  ?INF("cast_unknown", Msg),
  {noreply, State, 0}.

info_unknown(State, Msg) ->
  ?INF("info_unknown", Msg),
  {noreply, State, 0}.

call_unknown(State, Msg) ->
  ?INF("call_unknown", Msg),
  {reply, ?e(unknown_call), State, 0}.
%% }}}
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%



%%
query_(S = #{pool     := Pool, 
             in       := In, 
             until    := Un, 
             in_count := IC, 
             in_rate  := IR,
             w_seq    := Seq}, From, {Sql, Timeout}) ->
  Ref = erlang:make_ref(),
  Now   = ?millisecond,
  NewIn  = [{Ref, Sql, Timeout}|In],
  case In of [] -> get_worker(Pool, NewIn, Seq, Seq+1); _ -> do_nothing end,
  NewUn  = ordsets:add_element({Now + Timeout, Ref, {req, From}}, Un),
  [{LastUntil,_,_}|_] = NewUn, 
  {noreply, S#{in := NewIn, until := NewUn, in_count := IC+1, in_rate := mavg:event(IR)}, t(LastUntil, Now)}.
  

%
%worker(Pid, Worker) -> gen_server:cast(Pid, {worker, Worker}).
%%
worker_(S = #{pool := Pool, in := In, until := Un, workers_rate := WR, w_seq := Seq}, Worker, Lease, WSeq) ->
  case Worker of
    {ok, Pid} -> 

      Self = self(),
      SendFun = fun
        (Fu, [T|Tasks]) -> gen_server:cast(Pid, {query, Self, T}), Fu(Fu, Tasks);
        (_F, []) -> ok
      end,

      ProcessReqsFun = fun
        (Fu, RIn = [Req = {_Ref, _Sql, Timeout}|Rest], TSum, Acc) ->
          case TSum + Timeout of
            NewTSum when NewTSum < Lease -> Fu(Fu, Rest, NewTSum, [Req|Acc]);
            _ -> SendFun(SendFun, Acc), lists:reverse(RIn)
          end;
        (_F, [], _, [])  -> [];
        (_F, [], _, Acc) -> SendFun(SendFun, Acc), []
      end,

      NewIn = ProcessReqsFun(ProcessReqsFun, lists:reverse(In), 0, []),
      NewSeq = get_worker(Pool, NewIn, Seq, WSeq),

      NewS = S#{in := NewIn, workers_rate := mavg:event(WR), w_seq := NewSeq},
      Now = ?millisecond,
      QTimeout = case Un of [] -> 100000; [{Until,_,_}|_] -> t(Until, Now) end,
      {noreply, NewS, QTimeout};
      
    Else ->
      ?INF("Get worker Pid error", Else),
      %case In /= [] of true -> get_worker(Pool, In); false -> do_nothing end,
      Now = ?millisecond,
      QTimeout = case Un of [] -> 100000; [{Until,_,_}|_] -> t(Until, Now) end,
      {noreply, S, QTimeout}
  end.


%
%answer(Queue, Answer) -> gen_server:cast(Queue, {answer, Answer}).
%
answer_(S = #{until := Un, ok_count := OC, ok_rate := OR}, Answer) ->
  case Answer of
    {Ref, Body} ->
      {NewUn, NewOC, NewOR} = case lists:keytake(Ref, 2, Un) of
        {value, {_Until, Ref, {req, From}}, RestUn} -> 
          gen_server:reply(From, Body), {RestUn, OC+1, mavg:event(OR)};
        false -> do_nothing, {Un, OC, OR}
      end,
      Now = ?millisecond,
      QTimeout = case NewUn of [] -> 100000; [{Until,_,_}|_] -> t(Until, Now) end,
      {noreply, S#{until := NewUn, ok_count := NewOC, ok_rate := NewOR}, QTimeout};
    Else ->
      ?INF("Wrong Answer", Else),
      Now = ?millisecond,
      QTimeout = case Un of [] -> 100000; [{Until,_,_}|_] -> t(Until, Now) end,
      {noreply, S, QTimeout}
  end.



%% MISC
t(Until, Now) -> case Until - Now of T when T > 0 -> T; _ -> 0 end.


get_worker(_Pool, [], _Seq, WSeq) -> do_nothing, WSeq;
get_worker(_Pool, _In, Seq, WSeq) when Seq == WSeq -> do_nothing, WSeq;
get_worker(Pool, In, _Seq, WSeq) ->
  Q = self(),

  spawn(
    fun() -> 
      TimeoutList = [Timeout || {_,_,Timeout} <- In],
      QueueLease  = lists:sum(TimeoutList),
      MaxTimeout  = lists:max(TimeoutList),
      QueueSize   = length(TimeoutList),

      {CalcLease, CalcWorkers} = if 
        QueueSize > 160 -> well_done,   {MaxTimeout * 12, 15};
        QueueSize > 80  -> medium_well, {MaxTimeout * 8,  12};
        QueueSize > 40  -> medium,      {MaxTimeout * 6,  8};
        QueueSize > 20  -> medium_rare, {MaxTimeout * 4,  4};
        true            -> rare,        {MaxTimeout * 2,  2}
      end,

      Workers = case ers:stat(Pool) of
        #{free_conns := FreeConns} when FreeConns >= 0, FreeConns < CalcWorkers -> FreeConns;
        #{free_conns := FreeConns} when FreeConns >= CalcWorkers -> CalcWorkers;
        Else ->
          ?INF("Pool Error", Else), 0
      end,
      
      Lease = if CalcLease > QueueLease -> QueueLease; true -> CalcLease end,
      GetWorkerFun = fun
        (_F, 0) -> ok;
        (Fu, N) -> gen_server:cast(Q, {worker, ers:get_conn(Pool), Lease + 500, WSeq+1}), Fu(Fu, N-1)
      end,
      GetWorkerFun(GetWorkerFun, Workers)

    end),
  WSeq.



%% 
%% TODO
%% 1. queue stats
stat_(S = #{until         := Un,
            in            := In,
            in_count      := IC,
            ok_count      := OC,
            timeout_count := TC,
            in_rate       := IR,
            ok_rate       := OR,
            workers_rate  := WR,
            timeout_rate  := TR}) ->
  Stat = #{
    incoming      => length(In),
    requests      => length(Un),
    in_count      => IC,
    in_rate       => mavg:rate(IR),      %%
    ok_count      => OC,
    ok_rate       => mavg:rate(OR),      %%
    workers_rate  => mavg:rate(WR),      %%
    timeout_count => TC,
    timeout_rate  => mavg:rate(TR)
  },
  {reply, Stat, S, 0}.
