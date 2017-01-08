eqpg
=====

An OTP application nonblocking queue to postgresql pool conections with statistics.


Build
-----

    $ rebar3 compile


Usage
-----

% Get list of running queues
eqpg:list().


% create new queue, request and stop 
{ok, Q} = eqpg:create(Queue, Pool),
Answer = egpg:query(Q, Query),
eqpg:delete(Q),
Answer. 


% Get Stat for queue
egpg:stat(Q).

