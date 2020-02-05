-module(rabbit_classic_queue).
-behaviour(rabbit_queue_type).

-include("amqqueue.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-record(?MODULE, {pid :: undefined | pid(), %% the current master pid
                  qref :: term(), %% TODO
                  unconfirmed = #{} :: #{non_neg_integer() => [pid()]}}).
-define(STATE, ?MODULE).

-opaque state() :: #?STATE{}.

-export_type([state/0]).

-export([
         is_enabled/0,
         declare/2,
         delete/4,
         is_recoverable/1,
         recover/2,
         purge/1,
         policy_changed/1,
         stat/1,
         init/1,
         consume/3,
         cancel/5,
         handle_event/2,
         deliver/2,
         settle/3,
         reject/4,
         credit/4,
         dequeue/4,
         info/2,
         state_info/1
         ]).

-export([delete_crashed/1,
         delete_crashed/2,
         delete_crashed_internal/2]).

is_enabled() -> true.

declare(Q, Node) when ?amqqueue_is_classic(Q) ->
    QName = amqqueue:get_name(Q),
    VHost = amqqueue:get_vhost(Q),
    Node1 = case rabbit_queue_master_location_misc:get_location(Q)  of
              {ok, Node0}  -> Node0;
              {error, _}   -> Node
            end,
    Node1 = rabbit_mirror_queue_misc:initial_queue_node(Q, Node1),
    case rabbit_vhost_sup_sup:get_vhost_sup(VHost, Node1) of
        {ok, _} ->
            gen_server2:call(
              rabbit_amqqueue_sup_sup:start_queue_process(Node1, Q, declare),
              {init, new}, infinity);
        {error, Error} ->
            rabbit_misc:protocol_error(internal_error,
                            "Cannot declare a queue '~s' on node '~s': ~255p",
                            [rabbit_misc:rs(QName), Node1, Error])
    end.

delete(Q, IfUnused, IfEmpty, ActingUser) when ?amqqueue_is_classic(Q) ->
    case wait_for_promoted_or_stopped(Q) of
        {promoted, Q1} ->
            QPid = amqqueue:get_pid(Q1),
            delegate:invoke(QPid, {gen_server2, call,
                                   [{delete, IfUnused, IfEmpty, ActingUser},
                                    infinity]});
        {stopped, Q1} ->
            #resource{name = Name, virtual_host = Vhost} = amqqueue:get_name(Q1),
            case IfEmpty of
                true ->
                    rabbit_log:error("Queue ~s in vhost ~s has its master node down and "
                                     "no mirrors available or eligible for promotion. "
                                     "The queue may be non-empty. "
                                     "Refusing to force-delete.",
                                     [Name, Vhost]),
                    {error, not_empty};
                false ->
                    rabbit_log:warning("Queue ~s in vhost ~s has its master node is down and "
                                       "no mirrors available or eligible for promotion. "
                                       "Forcing queue deletion.",
                                       [Name, Vhost]),
                    delete_crashed_internal(Q1, ActingUser),
                    {ok, 0}
            end;
        {error, not_found} ->
            %% Assume the queue was deleted
            {ok, 0}
    end.

is_recoverable(Q) when ?is_amqqueue(Q) ->
    Node = node(),
    Node =:= node(amqqueue:get_pid(Q)) andalso
    %% Terminations on node down will not remove the rabbit_queue
    %% record if it is a mirrored queue (such info is now obtained from
    %% the policy). Thus, we must check if the local pid is alive
    %% - if the record is present - in order to restart.
    (mnesia:read(rabbit_queue, amqqueue:get_name(Q), read) =:= []
     orelse not rabbit_mnesia:is_process_alive(amqqueue:get_pid(Q))).

recover(VHost, Queues) ->
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    %% We rely on BQ:start/1 returning the recovery terms in the same
    %% order as the supplied queue names, so that we can zip them together
    %% for further processing in recover_durable_queues.
    {ok, OrderedRecoveryTerms} =
        BQ:start(VHost, [amqqueue:get_name(Q) || Q <- Queues]),
    case rabbit_amqqueue_sup_sup:start_for_vhost(VHost) of
        {ok, _}         ->
            RecoveredQs = recover_durable_queues(lists:zip(Queues,
                                                           OrderedRecoveryTerms)),
            RecoveredNames = [amqqueue:get_name(Q) || Q <- RecoveredQs],
            FailedQueues = [Q || Q <- Queues,
                                 not lists:member(amqqueue:get_name(Q), RecoveredNames)],
            {RecoveredQs, FailedQueues};
        {error, Reason} ->
            rabbit_log:error("Failed to start queue supervisor for vhost '~s': ~s", [VHost, Reason]),
            throw({error, Reason})
    end.

-spec policy_changed(amqqueue:amqqueue()) -> ok.
policy_changed(Q) ->
    QPid = amqqueue:get_pid(Q),
    gen_server2:cast(QPid, policy_changed).

stat(Q) ->
    delegate:invoke(amqqueue:get_pid(Q),
                    {gen_server2, call, [stat, infinity]}).

-spec init(amqqueue:amqqueue()) -> state().
init(Q) when ?amqqueue_is_classic(Q) ->
    #?STATE{pid = amqqueue:get_pid(Q),
            qref = amqqueue:get_name(Q)}.

consume(Q, Spec, State) when ?amqqueue_is_classic(Q) ->
    QPid = amqqueue:get_pid(Q),
    QRef = amqqueue:get_name(Q),
    #{no_ack := NoAck,
      channel_pid := ChPid,
      limiter_pid := LimiterPid,
      limiter_active := LimiterActive,
      prefetch_count := ConsumerPrefetchCount,
      consumer_tag := ConsumerTag,
      exclusive_consume := ExclusiveConsume,
      args := Args,
      ok_msg := OkMsg,
      acting_user :=  ActingUser} = Spec,
    case delegate:invoke(QPid,
                         {gen_server2, call,
                          [{basic_consume, NoAck, ChPid, LimiterPid,
                            LimiterActive, ConsumerPrefetchCount, ConsumerTag,
                            ExclusiveConsume, Args, OkMsg, ActingUser},
                           infinity]}) of
        ok ->
            %% ask the host process to monitor this pid
            %% TODO: track pids as they change
            {ok, State#?STATE{pid = QPid}, [{monitor, QPid, QRef}]};
        Err ->
            Err
    end.

cancel(Q, ConsumerTag, OkMsg, ActingUser, State) ->
    QPid = amqqueue:get_pid(Q),
    case delegate:invoke(QPid, {gen_server2, call,
                                [{basic_cancel, self(), ConsumerTag,
                                  OkMsg, ActingUser}, infinity]}) of
        ok ->
            {ok, State};
        Err -> Err
    end.

-spec settle(rabbit_types:ctag(), [non_neg_integer()], state()) ->
    state().
settle(_CTag, MsgIds, State) ->
    delegate:invoke_no_result(State#?STATE.pid,
                              {gen_server2, cast, [{ack, MsgIds, self()}]}),
    State.

reject(_CTag, Requeue, MsgIds, State) ->
    ChPid = self(),
    ok = delegate:invoke_no_result(State#?STATE.pid,
                                   {gen_server2, cast,
                                    [{reject, Requeue, MsgIds, ChPid}]}),
    State.

credit(CTag, Credit, Drain, State) ->
    ChPid = self(),
    delegate:invoke_no_result(State#?STATE.pid,
                              {gen_server2, cast,
                               [{credit, ChPid, CTag, Credit, Drain}]}),
    State.

handle_event({confirm, MsgSeqNos, Pid}, #?STATE{qref = QRef,
                                                unconfirmed = U0} = State) ->
    rabbit_log:info("handle_event classic ~w unconfirmed ~w", [MsgSeqNos, U0]),
    {Unconfirmed, ConfirmedSeqNos} = confirm_seq_nos(MsgSeqNos, Pid, U0),
    Actions = [{settled, QRef, ConfirmedSeqNos}],
    %% handle confirm event from queues
    %% in this case the classic queue should track each individual publish and
    %% the processes involved and only emit a settle action once they have all
    %% been received (or DOWN has been received).
    %% Hence this part of the confirm logic is queue specific.
    {ok, State#?STATE{unconfirmed = Unconfirmed}, Actions};
handle_event({reject_publish, SeqNo, _QPid},
              #?STATE{qref = QRef,
                      unconfirmed = U0} = State) ->
    rabbit_log:info("classic reject_publish ~w", [SeqNo]),
    %% It does not matter which queue rejected the message,
    %% if any queue did, it should not be confirmed.
    {U, Rejected} = reject_seq_no(SeqNo, U0),
    Actions = [{rejected, QRef, Rejected}],
    {ok, State#?STATE{unconfirmed = U}, Actions};
handle_event({down, Pid, Info}, #?STATE{qref = QRef,
                                        unconfirmed = U0} = State0) ->
    rabbit_log:info("classic: handle down ~w", [Info]),
    case rabbit_misc:is_abnormal_exit(Info) of
        false when Info =:= normal ->
            MsgSeqNos = maps:keys(
                          maps:filter(fun (_, Pids) ->
                                              lists:member(Pid, Pids)
                                      end, U0)),
            %% if the exit is normal, treat it as a "confirm"
            {Unconfirmed, ConfirmedSeqNos} = confirm_seq_nos(MsgSeqNos, Pid, U0),
            Actions = [{settled, QRef, ConfirmedSeqNos}],
            {ok, State0#?STATE{unconfirmed = Unconfirmed}, Actions};
        _ ->
            %% any abnormal exit should be considered a full reject of the
            %% oustanding message ids.
            MsgIds = maps:fold(
                          fun (SeqNo, Pids, Acc) ->
                                  case lists:member(Pid, Pids) of
                                      true ->
                                          [SeqNo | Acc];
                                      false ->
                                          Acc
                                  end
                          end, [], U0),
            U = maps:without(MsgIds, U0),
            rabbit_log:info("classic: reject MXs ~w unconfired ~w",
                            [MsgIds, U0]),
            {ok, State0#?STATE{unconfirmed = U}, [{rejected, QRef, MsgIds}]}
    end.
        % {false, _} ->
        %     {ConfirmMXs, RejectMXs, UC1} =
        %     unconfirmed_messages:forget_ref(QPid, UC),
        %     State1 = record_confirms(ConfirmMXs,
        %                              State#ch{unconfirmed = UC1}),
        %     record_rejects(RejectMXs, State1)
        %     end.

-spec deliver([{amqqueue:amqqueue(), state()}],
              Delivery :: term()) ->
    {[{amqqueue:amqqueue(), state()}], rabbit_queue_type:actions()}.
deliver(Qs0, #delivery{flow = Flow,
                       msg_seq_no = MsgNo,
                       message = #basic_message{exchange_name = _Ex},
                       confirm = _Confirm} = Delivery) ->
    %% TODO: record master and slaves for confirm processing
    {MPids, SPids, Qs, Actions} = qpids(Qs0, MsgNo),
    QPids = MPids ++ SPids,
    case Flow of
        %% Here we are tracking messages sent by the rabbit_channel
        %% process. We are accessing the rabbit_channel process
        %% dictionary.
        flow   -> [credit_flow:send(QPid) || QPid <- QPids],
                  [credit_flow:send(QPid) || QPid <- SPids];
        noflow -> ok
    end,
    MMsg = {deliver, Delivery, false},
    SMsg = {deliver, Delivery, true},
    delegate:invoke_no_result(MPids, {gen_server2, cast, [MMsg]}),
    delegate:invoke_no_result(SPids, {gen_server2, cast, [SMsg]}),
    {Qs, Actions}.


-spec dequeue(NoAck :: boolean(), LimiterPid :: pid(),
              rabbit_types:ctag(), state()) ->
    {ok, Count :: non_neg_integer(), rabbit_amqqueue:qmsg(), state()} |
    {empty, state()}.
dequeue(NoAck, LimiterPid, _CTag, State) ->
    QPid = State#?STATE.pid,
    case delegate:invoke(QPid, {gen_server2, call,
                           [{basic_get, self(), NoAck, LimiterPid}, infinity]}) of
        empty ->
            {empty, State};
        {ok, Count, Msg} ->
            {ok, Count, Msg, State}
    end.

-spec state_info(state()) -> #{atom() := term()}.
state_info(_State) ->
    #{}.

%% general queue info
-spec info(amqqueue:amqqueue(), all_keys | rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Q, Items) ->
    QPid = amqqueue:get_pid(Q),
    Req = case Items of
              all_keys -> info;
              _ -> {info, Items}
          end,
    case delegate:invoke(QPid, {gen_server2, call, [Req, infinity]}) of
        {ok, Result} ->
            Result;
        {error, _Err} ->
            []
    end.

-spec purge(amqqueue:amqqueue()) ->
    {ok, non_neg_integer()}.
purge(Q) when ?is_amqqueue(Q) ->
    QPid = amqqueue:get_pid(Q),
    delegate:invoke(QPid, {gen_server2, call, [purge, infinity]}).

qpids(Qs, MsgNo) ->
    % rabbit_log:info("classic deliver ~w", [MsgNo]),
    lists:foldl(
      fun ({Q, S0}, {MPidAcc, SPidAcc, Qs0, Actions0}) ->
              QPid = amqqueue:get_pid(Q),
              SPids = amqqueue:get_slave_pids(Q),
              QRef = amqqueue:get_name(Q),
              Actions = [{monitor, QPid, QRef}
                         | [{monitor, P, QRef} || P <- SPids]] ++ Actions0,
              %% confirm record
              S = case S0 of
                      #?STATE{unconfirmed = U0} ->
                          Rec = [QPid | SPids],
                          U = U0#{MsgNo => Rec},
                          S0#?STATE{unconfirmed = U};
                      stateless ->
                          S0
                  end,
              {[QPid | MPidAcc],
               SPidAcc ++ SPids,
               [{Q, S} | Qs0],
               Actions}
      end, {[], [], [], []}, Qs).

%% internal-ish
-spec wait_for_promoted_or_stopped(amqqueue:amqqueue()) ->
    {promoted, amqqueue:amqqueue()} |
    {stopped, amqqueue:amqqueue()} |
    {error, not_found}.
wait_for_promoted_or_stopped(Q0) ->
    QName = amqqueue:get_name(Q0),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            QPid = amqqueue:get_pid(Q),
            SPids = amqqueue:get_slave_pids(Q),
            case rabbit_mnesia:is_process_alive(QPid) of
                true  -> {promoted, Q};
                false ->
                    case lists:any(fun(Pid) ->
                                       rabbit_mnesia:is_process_alive(Pid)
                                   end, SPids) of
                        %% There is a live slave. May be promoted
                        true ->
                            timer:sleep(100),
                            wait_for_promoted_or_stopped(Q);
                        %% All slave pids are stopped.
                        %% No process left for the queue
                        false -> {stopped, Q}
                    end
            end;
        {error, not_found} ->
            {error, not_found}
    end.

-spec delete_crashed(amqqueue:amqqueue()) -> ok.
delete_crashed(Q) ->
    delete_crashed(Q, ?INTERNAL_USER).

delete_crashed(Q, ActingUser) ->
    ok = rpc:call(amqqueue:qnode(Q), ?MODULE, delete_crashed_internal,
                  [Q, ActingUser]).

delete_crashed_internal(Q, ActingUser) ->
    QName = amqqueue:get_name(Q),
    {ok, BQ} = application:get_env(rabbit, backing_queue_module),
    BQ:delete_crashed(Q),
    ok = rabbit_amqqueue:internal_delete(QName, ActingUser).

recover_durable_queues(QueuesAndRecoveryTerms) ->
    {Results, Failures} =
        gen_server2:mcall(
          [{rabbit_amqqueue_sup_sup:start_queue_process(node(), Q, recovery),
            {init, {self(), Terms}}} || {Q, Terms} <- QueuesAndRecoveryTerms]),
    [rabbit_log:error("Queue ~p failed to initialise: ~p~n",
                      [Pid, Error]) || {Pid, Error} <- Failures],
    [Q || {_, {new, Q}} <- Results].


reject_seq_no(SeqNo, U0) ->
    reject_seq_no(SeqNo, U0, []).

reject_seq_no(SeqNo, U0, Acc) ->
    case maps:take(SeqNo, U0) of
        {_, U} ->
            {U, [SeqNo | Acc]};
        error ->
            {U0, Acc}
    end.

confirm_seq_nos(MsgSeqNos, Pid, U0) ->
    lists:foldl(
      fun (SeqNo, {U, A0}) ->
              case U of
                  #{SeqNo := Pids0} ->
                      case lists:delete(Pid, Pids0) of
                          [] ->
                              % rabbit_log:info("handle_event empty ~w", [SeqNo]),
                              %% the updated unconfirmed state
                              %% and the seqnos to settle
                              {maps:remove(SeqNo, U), [SeqNo | A0]};
                          Pids ->
                              {U#{SeqNo => Pids}, A0}
                      end;
                  _ ->
                      {U, A0}
              end
      end, {U0, []}, MsgSeqNos).
