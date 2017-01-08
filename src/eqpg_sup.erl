%%%-------------------------------------------------------------------
%% @doc eqpg top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(eqpg_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

% 
-export([start_queue/1, stop_queue/1]).

-include("../include/eqpg.hrl").

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->

    SupFlags = #{strategy => one_for_one,
                 intensity => 10,
                 period => 1},

    ChildSpecs = [],

    {ok, {SupFlags, ChildSpecs}}.


%%====================================================================
%% Internal functions
%%====================================================================
start_queue(#{name := QName, mfa := {M,F,A}}) ->
  ChildSpec = #{
    id        => QName,      % 
    start     => {M,F,A},    % 
    restart   => transient,  % 
    shutdown  => 5000,       % 
    type      => worker,     % 
    modules   => [M]},       % 

  supervisor:start_child(?MODULE, ChildSpec).



%
stop_queue(Pid) when is_pid(Pid) ->
  case [Id || {Id, P, _, _} <- supervisor:which_children(?MODULE), P == Pid] of
    [Queue] -> stop_queue(Queue);
    []      -> ?e(no_such_queue)
  end;
stop_queue(Queue) ->
  case supervisor:terminate_child(?MODULE, Queue) of
    ok ->
      case supervisor:delete_child(?MODULE, Queue) of
        ok -> ok;
        {error, not_found} -> ?e(no_such_queue);
        Else               -> ?e(unknown_error, Else)
      end;
    {error, not_found} -> ?e(no_such_queue);
    Else               -> ?e(unknown_error, Else)
  end.
  
