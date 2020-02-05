-module(rabbit_queue_type).
-include("amqqueue.hrl").
-include_lib("rabbit_common/include/resource.hrl").

-export([
         init/0,
         discover/1,
         default/0,
         is_enabled/1,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         % link_name/3,
         remove/2,
         info/2,
         state_info/1,
         info_down/3,
         %% stateful client API
         new/2,
         consume/3,
         cancel/5,
         handle_down/3,
         handle_event/3,
         deliver/3,
         settle/4,
         reject/5,
         credit/5,
         dequeue/5

         ]).

%% temporary
-export([with/3]).

%% gah what is a good identity of a classic queue including all replicas
-type queue_name() :: rabbit_types:r(queue).
-type queue_ref() :: queue_name() | atom().
-type queue_state() :: term().
-type msg_tag() :: term().

-define(STATE, ?MODULE).

-define(QREF(QueueReference),
    (is_tuple(QueueReference) andalso element(1, QueueReference) == resource)
    orelse is_atom(QueueReference)).
%% anything that the host process needs to do on behalf of the queue type
%% session, like knowing when to notify on monitor down
-type action() ::
    {monitor, Pid :: pid(), queue_ref()} |
    %% indicate to the queue type module that a message has been delivered
    %% fully to the queue
    {settled, Success :: boolean(), [msg_tag()]} |
    {deliver, rabbit_type:ctag(), boolean(), [rabbit_amqqueue:qmsg()]}.

-type actions() :: [action()].

-type event() ::
    {down, pid(), Info :: term()} |
    term().

-record(ctx, {module :: module(),
              name :: queue_name(),
              %% "publisher confirm queue accounting"
              %% queue type implementation should emit a:
              %% {settle, Success :: boolean(), msg_tag()}
              %% to either settle or reject the delivery of a
              %% message to the queue instance
              %% The queue type module will then emit a {confirm | reject, [msg_tag()}
              %% action to the channel or channel like process when a msg_tag
              %% has reached its conclusion
              % unsettled = #{} :: #{msg_tag() => [queue_ref()]},
              state :: queue_state()}).


-record(?STATE, {ctxs = #{} :: #{queue_ref() => #ctx{} | queue_ref()},
                 monitor_registry = #{} :: #{pid() => queue_ref()}
                }).

-opaque state() :: #?STATE{}.

-type consume_spec() :: #{no_ack := boolean(),
                          channel_pid := pid(),
                          limiter_pid => pid(),
                          limiter_active => boolean(),
                          prefetch_count => non_neg_integer(),
                          consumer_tag := rabbit_types:ctag(),
                          exclusive_consume => boolean(),
                          args => rabbit_framing:amqp_table(),
                          ok_msg := term(),
                          acting_user :=  rabbit_types:username()}.


-export_type([state/0,
              consume_spec/0,
              action/0,
              actions/0]).


% copied from rabbit_amqqueue
-type absent_reason() :: 'nodedown' | 'crashed' | stopped | timeout.

%% is the queue type feature enabled
-callback is_enabled() -> boolean().

-callback declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().

-callback delete(amqqueue:amqqueue(),
                 boolean(),
                 boolean(),
                 rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).

-callback recover(rabbit_types:vhost(), [amqqueue:amqqueue()]) ->
    {Recovered :: [amqqueue:amqqueue()],
     Failed :: [amqqueue:amqqueue()]}.

%% checks if the queue should be recovered
-callback is_recoverable(amqqueue:amqqueue()) ->
    boolean().

-callback purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()} | {error, term()}.

-callback policy_changed(amqqueue:amqqueue()) -> ok.

%% stateful
%% intitialise and return a queue type specific session context
-callback init(amqqueue:amqqueue()) -> queue_state().

-callback consume(amqqueue:amqqueue(),
                  consume_spec(),
                  queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()}.

-callback cancel(amqqueue:amqqueue(),
                 rabbit_types:ctag(),
                 term(),
                 rabbit_types:username(),
                 queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()}.

%% any async events returned from the queue system should be processed through
%% this
-callback handle_event(Event :: event(),
                       queue_state()) ->
    {ok, queue_state(), actions()} | {error, term()} | eol.

-callback deliver([{amqqueue:amqqueue(), queue_state()}],
                  Delivery :: term()) ->
    {[{amqqueue:amqqueue(), queue_state()}], actions()}.

-callback settle(rabbit_types:ctag(), [non_neg_integer()], queue_state()) ->
    queue_state().

-callback reject(rabbit_types:ctag(), Requeue :: boolean(),
                 MsgIds :: [non_neg_integer()], queue_state()) ->
    queue_state().

-callback credit(rabbit_types:ctag(),
                 non_neg_integer(), Drain :: boolean(), queue_state()) ->
    queue_state().

-callback dequeue(NoAck :: boolean(), LimiterPid :: pid(),
                  rabbit_types:ctag(), queue_state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), queue_state()} |
    {empty, queue_state()} |
    {error, term()}.

%% return a map of state summary information
-callback state_info(queue_state()) ->
    #{atom() := term()}.

%% general queue info
-callback info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().

%% TODO: this should be controlled by a registry that is populated on boot
discover(<<"quorum">>) ->
    rabbit_quorum_queue;
discover(<<"classic">>) ->
    rabbit_classic_queue.

default() ->
    rabbit_classic_queue.

-spec is_enabled(module()) -> boolean().
is_enabled(Type) ->
    Type:is_enabled().

-spec declare(amqqueue:amqqueue(), node()) ->
    {'new' | 'existing' | 'owner_died', amqqueue:amqqueue()} |
    {'absent', amqqueue:amqqueue(), absent_reason()} |
    rabbit_types:channel_exit().
declare(Q, Node) ->
    Mod = amqqueue:get_type(Q),
    Mod:declare(Q, Node).

-spec delete(amqqueue:amqqueue(), boolean(),
             boolean(), rabbit_types:username()) ->
    rabbit_types:ok(non_neg_integer()) |
    rabbit_types:error(in_use | not_empty).
delete(Q, IfUnused, IfEmpty, ActingUser) ->
    Mod = amqqueue:get_type(Q),
    Mod:delete(Q, IfUnused, IfEmpty, ActingUser).

-spec purge(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer()}.
purge(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:purge(Q).

-spec policy_changed(amqqueue:amqqueue()) -> 'ok'.
policy_changed(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:policy_changed(Q).

-spec stat(amqqueue:amqqueue()) ->
    {'ok', non_neg_integer(), non_neg_integer()}.
stat(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:stat(Q).

% register_monitored_pid(QRef, Pid, State) ->
%     case Ctxs of
%         #{QRef := ParentQRef} when ?QREF(ParentQRef) ->
%             %% already registered
%             Ctxs;
%         #{ParentQRef := #ctx{}} ->
%             %% link together
%             Ctxs#{QRef => ParentQRef};
%         _ ->
%             exit(parent_queue_ref_not_found)
%     end.

-spec remove(queue_ref(), state()) -> state().
remove(QRef, #?STATE{ctxs = Ctxs0} = State) ->
    case maps:take(QRef, Ctxs0) of
        error ->
            State#?STATE{ctxs = Ctxs0};
        {_, Ctxs} ->
            %% remove all linked queue refs
            State#?STATE{ctxs = maps:filter(fun (_, V) ->
                                                    V == QRef
                                            end, Ctxs)}
    end.


-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Q, Items) when ?amqqueue_state_is(Q, crashed) ->
    info_down(Q, Items, crashed);
info(Q, Items) when ?amqqueue_state_is(Q, stopped) ->
    info_down(Q, Items, stopped);
info(Q, Items) ->
    Mod = amqqueue:get_type(Q),
    Mod:info(Q, Items).

state_info(#ctx{state = S,
                module = Mod}) ->
    Mod:state_info(S);
state_info(_) ->
    #{}.

info_down(Q, all_keys, DownReason) ->
    info_down(Q, rabbit_amqqueue_process:info_keys(), DownReason);
info_down(Q, Items, DownReason) ->
    [{Item, i_down(Item, Q, DownReason)} || Item <- Items].

i_down(name,               Q, _) -> amqqueue:get_name(Q);
i_down(durable,            Q, _) -> amqqueue:is_durable(Q);
i_down(auto_delete,        Q, _) -> amqqueue:is_auto_delete(Q);
i_down(arguments,          Q, _) -> amqqueue:get_arguments(Q);
i_down(pid,                Q, _) -> amqqueue:get_pid(Q);
i_down(recoverable_slaves, Q, _) -> amqqueue:get_recoverable_slaves(Q);
i_down(type,               Q, _) -> amqqueue:get_type(Q);
i_down(state, _Q, DownReason)    -> DownReason;
i_down(K, _Q, _DownReason) ->
    case lists:member(K, rabbit_amqqueue_process:info_keys()) of
        true  -> '';
        false -> throw({bad_argument, K})
    end.

-spec init() -> state().
init() ->
    #?STATE{}.

-spec new(amqqueue:amqqueue(), state()) -> state().
new(Q, State) when ?is_amqqueue(Q) ->
    Ctx = get_ctx(Q, State),
    set_ctx(Q, Ctx, State).

-spec consume(amqqueue:amqqueue(), consume_spec(), state()) ->
    {ok, state(), actions()} | {error, term()}.
consume(Q, Spec, State) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, State),
    Mod = amqqueue:get_type(Q),
    case Mod:consume(Q, Spec, State0) of
        {ok, CtxState, Actions} ->
            return_ok(set_ctx(Q, Ctx#ctx{state = CtxState}, State), Actions);
        Err ->
            Err
    end.

%% TODO switch to cancel spec api
-spec cancel(amqqueue:amqqueue(),
             rabbit_types:ctag(),
             term(),
             rabbit_types:username(),
             state()) ->
    {ok, state()} | {error, term()}.
cancel(Q, Tag, OkMsg, ActiveUser, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:cancel(Q, Tag, OkMsg, ActiveUser, State0) of
        {ok, State} ->
            {ok, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        Err ->
            Err
    end.

-spec is_recoverable(amqqueue:amqqueue()) ->
    boolean().
is_recoverable(Q) ->
    Mod = amqqueue:get_type(Q),
    Mod:is_recoverable(Q).


-spec recover(rabbit_types:vhost(), [amqqueue:amqqueue()]) ->
    {Recovered :: [amqqueue:amqqueue()],
     Failed :: [amqqueue:amqqueue()]}.
recover(VHost, Qs) ->
   ByType = lists:foldl(
              fun (Q, Acc) ->
                      T = amqqueue:get_type(Q),
                      maps:update_with(T, fun (X) ->
                                                  [Q | X]
                                          end, Acc)
                      %% TODO resolve all registered queue types from registry
              end, #{rabbit_classic_queue => [],
                     rabbit_quorum_queue => []}, Qs),
   maps:fold(fun (Mod, Queues, {R0, F0}) ->
                     {R, F} = Mod:recover(VHost, Queues),
                     {R0 ++ R, F0 ++ F}
             end, {[], []}, ByType).

-spec handle_down(pid(), term(), state()) ->
    {ok, state(), actions()} | eol | {error, term()}.
handle_down(Pid, Info, #?STATE{monitor_registry = Reg} = State0) ->
    %% lookup queue ref in monitor registry
    case Reg of
        #{Pid := QRef} ->
            %% TODO: remove Pid from monitor_registry
            case handle_event(QRef, {down, Pid, Info}, State0) of
                {ok, State, Actions} ->
                    {ok, State, [{queue_down, QRef} | Actions]};
                Err ->
                    Err
            end;
        _ ->
            {ok, State0, []}
    end.

%% messages sent from queues
-spec handle_event(queue_ref(), term(), state()) ->
    {ok, state(), actions()} | eol | {error, term()}.
handle_event(QRef, Evt, Ctxs) ->
    %% TODO: if the even is a down event we need to intercept it
    %% and translate the pid or mref to the queue ref and dispatch
    %%
    %% events can arrive after a queue state has been cleared up
    %% so need to be defensive here
    case get_ctx(QRef, Ctxs, undefined) of
        #ctx{module = Mod,
             state = State0} = Ctx  ->
            case Mod:handle_event(Evt, State0) of
                {ok, State, Actions} ->
                    return_ok(set_ctx(QRef, Ctx#ctx{state = State}, Ctxs), Actions);
                Err ->
                    Err
            end;
        undefined ->
            {ok, Ctxs, []}
    end.


-spec deliver([amqqueue:amqqueue()], Delivery :: term(),
              stateless | state()) ->
    {ok, state(), actions()}.
deliver(Qs, Delivery, stateless) ->
    _ = lists:map(fun(Q) ->
                          Mod = amqqueue:get_type(Q),
                          _ = Mod:deliver([{Q, stateless}], Delivery)
                  end, Qs),
    {ok, stateless, []};
deliver(Qs, Delivery, #?STATE{} = State0) ->
    %% sort by queue type - then dispatch each group
    ByType = lists:foldl(
               fun (Q, Acc) ->
                       T = amqqueue:get_type(Q),
                       Ctx = get_ctx(Q, State0),
                       maps:update_with(
                         T, fun (A) ->
                                    Ctx = get_ctx(Q, State0),
                                    [{Q, Ctx#ctx.state} | A]
                            end, [{Q, Ctx#ctx.state}], Acc)
               end, #{}, Qs),
    %%% dispatch each group to queue type interface?
    {Xs, Actions} = maps:fold(fun(Mod, QSs, {X0, A0}) ->
                                      {X, A} = Mod:deliver(QSs, Delivery),
                                      {X0 ++ X, A0 ++ A}
                              end, {[], []}, ByType),
    State = lists:foldl(
              fun({Q, S}, Acc) ->
                      Ctx = get_ctx(Q, Acc),
                      set_ctx(qref(Q), Ctx#ctx{state = S}, Acc)
              end, State0, Xs),
    return_ok(State, Actions).


-spec settle(queue_ref(), rabbit_types:ctag(),
             [non_neg_integer()], state()) -> state().
settle(QRef, CTag, MsgIds, Ctxs)
  when ?QREF(QRef) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QRef, Ctxs),
    State = Mod:settle(CTag, MsgIds, State0),
    set_ctx(QRef, Ctx#ctx{state = State}, Ctxs).

-spec reject(queue_ref(), rabbit_types:ctag(),
             boolean(), [non_neg_integer()], state()) -> state().
reject(QRef, CTag, Requeue, MsgIds, Ctxs)
  when ?QREF(QRef) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(QRef, Ctxs),
    State = Mod:reject(CTag, Requeue, MsgIds, State0),
    set_ctx(QRef, Ctx#ctx{state = State}, Ctxs).

-spec credit(amqqueue:amqqueue() | queue_ref(),
             rabbit_types:ctag(), non_neg_integer(),
             boolean(), state()) -> state().
credit(Q, CTag, Credit, Drain, Ctxs) ->
    #ctx{state = State0,
         module = Mod} = Ctx = get_ctx(Q, Ctxs),
    State = Mod:credit(CTag, Credit, Drain, State0),
    set_ctx(Q, Ctx#ctx{state = State}, Ctxs).

-spec dequeue(amqqueue:amqqueue(), boolean(),
              pid(), rabbit_types:ctag(), state()) ->
    {ok, non_neg_integer(), term(), state()}  |
    {empty, state()}.
dequeue(Q, NoAck, LimiterPid, CTag, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(Q, Ctxs),
    Mod = amqqueue:get_type(Q),
    case Mod:dequeue(NoAck, LimiterPid, CTag, State0) of
        {ok, Num, Msg, State} ->
            {ok, Num, Msg, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)};
        {empty, State} ->
            {empty, set_ctx(Q, Ctx#ctx{state = State}, Ctxs)}
    end.

%% temporary
with(QRef, Fun, Ctxs) ->
    #ctx{state = State0} = Ctx = get_ctx(QRef, Ctxs),
    {Res, State} = Fun(State0),
    {Res, set_ctx(QRef, Ctx#ctx{state = State}, Ctxs)}.


get_ctx(Q, #?STATE{ctxs =  Contexts}) when ?is_amqqueue(Q) ->
    Ref = qref(Q),
    case Contexts of
        #{Ref := #ctx{} = Ctx} ->
            Ctx;
        _ ->
            %% not found - initialize
            Mod = amqqueue:get_type(Q),
            Name = amqqueue:get_name(Q),
            #ctx{module = Mod,
                 name = Name,
                 state = Mod:init(Q)}
    end;
get_ctx(QRef, Contexts) when ?QREF(QRef) ->
    case get_ctx(QRef, Contexts, undefined) of
        undefined ->
            exit({queue_context_not_found, QRef});
        Ctx ->
            Ctx
    end.

get_ctx(QRef, #?STATE{ctxs = Contexts}, Default) ->
    Ref = qref(QRef),
    %% if we use a QRef it should always be initialised
    case maps:get(Ref, Contexts, undefined) of
        #ctx{} = Ctx ->
            Ctx;
        undefined ->
            Default
    end.

set_ctx(Q, Ctx, #?STATE{ctxs = Contexts} = State)
  when ?is_amqqueue(Q) ->
    Ref = qref(Q),
    State#?STATE{ctxs = maps:put(Ref, Ctx, Contexts)};
set_ctx(QRef, Ctx, #?STATE{ctxs = Contexts} = State) ->
    Ref = qref(QRef),
    State#?STATE{ctxs = maps:put(Ref, Ctx, Contexts)}.

qref(#resource{kind = queue} = QName) ->
    QName;
qref({Name, _}) -> Name;
qref(Q) when ?is_amqqueue(Q) ->
    amqqueue:get_name(Q);
%% assume it already is a ref
qref(Ref) -> Ref.


return_ok(State0, Actions0) ->
    {State, Actions} =
    lists:foldl(
      fun({monitor, Pid, QRef},
          {#?STATE{monitor_registry = M0} = S0, A0}) ->
              case M0 of
                  #{Pid := QRef} ->
                      %% already monitored by the qref
                      {S0, A0};
                  #{Pid := _} ->
                      %% TODO: allow multiple Qrefs to monitor the same pid
                      exit(return_ok_duplicate_montored_pid);
                  _ ->
                      rabbit_log:debug("queue type monitoring ~w", [Pid]),
                      _ = erlang:monitor(process, Pid),
                      M = M0#{Pid => QRef},
                      {S0#?STATE{monitor_registry = M}, A0}
              end;
         (Act, {S, A0}) ->
              {S, [Act | A0]}
      end, {State0, []}, Actions0),
    {ok, State, lists:reverse(Actions)}.
