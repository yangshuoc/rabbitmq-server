-module(rabbit_fifo_int_SUITE).

%% rabbit_fifo and rabbit_fifo_client integration suite

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

all() ->
    [
     {group, tests}
    ].

all_tests() ->
    [
     basics,
     return,
     rabbit_fifo_returns_correlation,
     resends_lost_command,
     returns_after_down,
     resends_after_lost_applied,
     handles_reject_notification,
     two_quick_enqueues,
     detects_lost_delivery,
     dequeue,
     discard,
     cancel_checkout,
     credit,
     untracked_enqueue,
     flow,
     test_queries,
     duplicate_delivery,
     usage
    ].

groups() ->
    [
     {tests, [], all_tests()}
    ].

init_per_group(_, Config) ->
    PrivDir = ?config(priv_dir, Config),
    _ = application:load(ra),
    ok = application:set_env(ra, data_dir, PrivDir),
    application:ensure_all_started(ra),
    application:ensure_all_started(lg),
    Config.

end_per_group(_, Config) ->
    _ = application:stop(ra),
    Config.

init_per_testcase(TestCase, Config) ->
    meck:new(rabbit_quorum_queue, [passthrough]),
    meck:expect(rabbit_quorum_queue, handle_tick, fun (_, _, _) -> ok end),
    meck:expect(rabbit_quorum_queue, cancel_consumer_handler,
                fun (_, _) -> ok end),
    ra_server_sup_sup:remove_all(),
    ServerName2 = list_to_atom(atom_to_list(TestCase) ++ "2"),
    ServerName3 = list_to_atom(atom_to_list(TestCase) ++ "3"),
    ClusterName = rabbit_misc:r("/", queue, atom_to_binary(TestCase, utf8)),
    [
     {cluster_name, ClusterName},
     {uid, atom_to_binary(TestCase, utf8)},
     {node_id, {TestCase, node()}},
     {uid2, atom_to_binary(ServerName2, utf8)},
     {node_id2, {ServerName2, node()}},
     {uid3, atom_to_binary(ServerName3, utf8)},
     {node_id3, {ServerName3, node()}}
     | Config].

end_per_testcase(_, Config) ->
    meck:unload(),
    Config.

basics(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    CustomerTag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    FState0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, FState1} = rabbit_fifo_client:checkout(CustomerTag, 1, #{}, FState0),

    ra_log_wal:force_roll_over(ra_log_wal),
    % create segment the segment will trigger a snapshot
    timer:sleep(1000),

    {ok, FState2} = rabbit_fifo_client:enqueue(one, FState1),
    % process ra events
    FState3 = process_ra_event(FState2, 250),

    FState5 = receive
                  {ra_event, From, Evt} ->
                      case rabbit_fifo_client:handle_ra_event(From, Evt, FState3) of
                          {ok, FState4,
                           [{deliver, C, true,
                             [{_Qname, _QRef, MsgId, _SomBool, _Msg}]}]} ->
                              rabbit_fifo_client:settle(C, [MsgId], FState4)
                      end
              after 5000 ->
                        exit(await_msg_timeout)
              end,

    % process settle applied notification
    FState5b = process_ra_event(FState5, 250),
    _ = ra:stop_server(ServerId),
    _ = ra:restart_server(ServerId),

    %% wait for leader change to notice server is up again
    receive
        {ra_event, _, {machine, leader_change}} -> ok
    after 5000 ->
              exit(leader_change_timeout)
    end,

    {ok, FState6} = rabbit_fifo_client:enqueue(two, FState5b),
    % process applied event
    FState6b = process_ra_event(FState6, 250),

    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, FState6b) of
                {ok, FState7, [{deliver, Ctag, true,
                                [{_, _, Mid, _, two}]}]} ->
                    _S = rabbit_fifo_client:return(Ctag, [Mid], FState7),
                    ok
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

return(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ServerId2 = ?config(node_id2, Config),
    ok = start_cluster(ClusterName, [ServerId, ServerId2]),

    F00 = rabbit_fifo_client:init(ClusterName, [ServerId, ServerId2]),
    {ok, F0} = rabbit_fifo_client:enqueue(1, msg1, F00),
    {ok, F1} = rabbit_fifo_client:enqueue(2, msg2, F0),
    {_, _, F2} = process_ra_events(F1, 100),
    {ok, _, {_, _, MsgId, _, _}, F} = rabbit_fifo_client:dequeue(<<"tag">>, unsettled, F2),
    _F2 = rabbit_fifo_client:return(<<"tag">>, [MsgId], F),

    ra:stop_server(ServerId),
    ok.

rabbit_fifo_returns_correlation(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(corr1, msg1, F0),
    receive
        {ra_event, Frm, E} ->
            case rabbit_fifo_client:handle_ra_event(Frm, E, F1) of
                {ok, _F2, [{settled, _, _}]} ->
                    ok;
                Del ->
                    exit({unexpected, Del})
            end
    after 2000 ->
              exit(await_msg_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

duplicate_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    Fun = fun Loop(S0) ->
            receive
                {ra_event, Frm, E} = Evt ->
                    case rabbit_fifo_client:handle_ra_event(Frm, E, S0) of
                        {ok, S1, [{settled, _, _}]} ->
                            Loop(S1);
                        {ok, S1, _} ->
                            %% repeat event delivery
                            self() ! Evt,
                            %% check that then next received delivery doesn't
                            %% repeat or crash
                            receive
                                {ra_event, F, E1} ->
                                    case rabbit_fifo_client:handle_ra_event(
                                           F, E1, S1) of
                                        {ok, S2, _} ->
                                            S2
                                    end
                            end
                    end
            after 2000 ->
                      exit(await_msg_timeout)
            end
        end,
    Fun(F2),
    ra:stop_server(ServerId),
    ok.

usage(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(corr1, msg1, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(corr2, msg2, F2),
    {_, _, _} = process_ra_events(F3, 50),
    % force tick and usage stats emission
    ServerId ! tick_timeout,
    timer:sleep(50),
    Use = rabbit_fifo:usage(element(1, ServerId)),
    ra:stop_server(ServerId),
    ?assert(Use > 0.0),
    ok.

resends_lost_command(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = meck:new(ra, [passthrough]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    % lose the enqueue
    meck:expect(ra, pipeline_command, fun (_, _, _) -> ok end),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    meck:unload(ra),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    {_, _, F4} = process_ra_events(F3, 500),
    {ok, _, {_, _, _, _, msg1}, F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, _, {_, _, _, _, msg2}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg3}, _F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    ra:stop_server(ServerId),
    ok.

two_quick_enqueues(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    F1 = element(2, rabbit_fifo_client:enqueue(msg1, F0)),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    _ = process_ra_events(F2, 500),
    ra:stop_server(ServerId),
    ok.

detects_lost_delivery(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F000 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F00} = rabbit_fifo_client:enqueue(msg1, F000),
    {_, _, F0} = process_ra_events(F00, 100),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    % lose first delivery
    receive
        {ra_event, _, {machine, {delivery, _, [{_, {_, msg1}}]}}} ->
            ok
    after 500 ->
              exit(await_delivery_timeout)
    end,

    % assert three deliveries were received
    {[_, _, _], _, _} = process_ra_events(F3, 500),
    ra:stop_server(ServerId),
    ok.

returns_after_down(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:enqueue(msg1, F0),
    {_, _, F2} = process_ra_events(F1, 500),
    % start a customer in a separate processes
    % that exits after checkout
    Self = self(),
    _Pid = spawn(fun () ->
                         F = rabbit_fifo_client:init(ClusterName, [ServerId]),
                         {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 10,
                                                               #{}, F),
                         Self ! checkout_done
                 end),
    receive checkout_done -> ok after 1000 -> exit(checkout_done_timeout) end,
    timer:sleep(1000),
    % message should be available for dequeue
    {ok, _, {_, _, _, _, msg1}, _} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F2),
    ra:stop_server(ServerId),
    ok.

resends_after_lost_applied(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {_, _, F1} = process_ra_events(element(2, rabbit_fifo_client:enqueue(msg1, F0)),
                           500),
    {ok, F2} = rabbit_fifo_client:enqueue(msg2, F1),
    % lose an applied event
    receive
        {ra_event, _, {applied, _}} ->
            ok
    after 500 ->
              exit(await_ra_event_timeout)
    end,
    % send another message
    {ok, F3} = rabbit_fifo_client:enqueue(msg3, F2),
    {_, _, F4} = process_ra_events(F3, 500),
    {ok, _, {_, _, _, _, msg1}, F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F4),
    {ok, _, {_, _, _, _, msg2}, F6} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F5),
    {ok, _, {_, _, _, _, msg3}, _F7} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F6),
    ra:stop_server(ServerId),
    ok.

handles_reject_notification(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId1 = ?config(node_id, Config),
    ServerId2 = ?config(node_id2, Config),
    UId1 = ?config(uid, Config),
    CId = {UId1, self()},

    ok = start_cluster(ClusterName, [ServerId1, ServerId2]),
    _ = ra:process_command(ServerId1,
                           rabbit_fifo:make_checkout(
                             CId,
                             {auto, 10, simple_prefetch},
                             #{})),
    % reverse order - should try the first node in the list first
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId2, ServerId1]),
    {ok, F1} = rabbit_fifo_client:enqueue(one, F0),

    timer:sleep(500),

    % the applied notification
    _F2 = process_ra_event(F1, 250),
    ra:stop_server(ServerId1),
    ra:stop_server(ServerId2),
    ok.

discard(Config) ->
    PrivDir = ?config(priv_dir, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    ClusterName = ?config(cluster_name, Config),
    Conf = #{cluster_name => ClusterName#resource.name,
             id => ServerId,
             uid => UId,
             log_init_args => #{data_dir => PrivDir, uid => UId},
             initial_member => [],
             machine => {module, rabbit_fifo,
                         #{queue_resource => discard,
                           dead_letter_handler =>
                           {?MODULE, dead_letter_handler, [self()]}}}},
    _ = ra:start_server(Conf),
    ok = ra:trigger_election(ServerId),
    _ = ra:members(ServerId),

    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, F1} = rabbit_fifo_client:checkout(<<"tag">>, 10, #{}, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(msg1, F1),
    F3 = discard_next_delivery(F2, 500),
    {empty, _F4} = rabbit_fifo_client:dequeue(<<"tag1">>, settled, F3),
    receive
        {dead_letter, Letters} ->
            [{_, msg1}] = Letters,
            ok
    after 500 ->
              flush(),
              exit(dead_letter_timeout)
    end,
    ra:stop_server(ServerId),
    ok.

cancel_checkout(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:checkout(<<"tag">>, 10, #{}, F1),
    {_, _, F3} = process_ra_events0(F2, [], [], 250, fun (_, S) -> S end),
    {ok, F4} = rabbit_fifo_client:cancel_checkout(<<"tag">>, F3),
    F5 = rabbit_fifo_client:return(<<"tag">>, [0], F4),
    {ok, _, {_, _, _, _, m1}, F5} = rabbit_fifo_client:dequeue(<<"d1">>, settled, F5),
    ok.

credit(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
    {_, _, F3} = process_ra_events(F2, [], 250),
    %% checkout with 0 prefetch
    {ok, F4} = rabbit_fifo_client:checkout(<<"tag">>, 0, credited, #{}, F3),
    %% assert no deliveries
    {_, _, F5} = process_ra_events0(F4, [], [], 250,
                                    fun
                                        (D, _) -> error({unexpected_delivery, D})
                                    end),
    %% provide some credit
    F6 = rabbit_fifo_client:credit(<<"tag">>, 1, false, F5),
    {[{_, _, _, _, m1}], [{send_credit_reply, _}], F7} =
        process_ra_events(F6, [], 250),

    %% credit and drain
    F8 = rabbit_fifo_client:credit(<<"tag">>, 4, true, F7),
    {[{_, _, _, _, m2}], [{send_credit_reply, _}, {send_drained, _}], F9} =
        process_ra_events(F8, [], 250),
    flush(),

    %% enqueue another message - at this point the consumer credit should be
    %% all used up due to the drain
    {ok, F10} = rabbit_fifo_client:enqueue(m3, F9),
    %% assert no deliveries
    {_, _, F11} = process_ra_events0(F10, [], [], 250,
                                   fun
                                       (D, _) -> error({unexpected_delivery, D})
                                   end),
    %% credit again and receive the last message
    F12 = rabbit_fifo_client:credit(<<"tag">>, 10, false, F11),
    {[{_, _, _, _, m3}], _, _} = process_ra_events(F12, [], 250),
    ok.

untracked_enqueue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),

    ok = rabbit_fifo_client:untracked_enqueue([ServerId], msg1),
    timer:sleep(100),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {ok, _, {_, _, _, _, msg1}, _F5} = rabbit_fifo_client:dequeue(<<"tag">>, settled, F0),
    ra:stop_server(ServerId),
    ok.


flow(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 3),
    {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
    {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
    {ok, F3} = rabbit_fifo_client:enqueue(m3, F2),
    {slow, F4} = rabbit_fifo_client:enqueue(m4, F3),
    {_, _, F5} = process_ra_events(F4, 500),
    {ok, _} = rabbit_fifo_client:enqueue(m5, F5),
    ra:stop_server(ServerId),
    ok.

test_queries(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    ok = start_cluster(ClusterName, [ServerId]),
    P = spawn(fun () ->
                  F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
                  {ok, F1} = rabbit_fifo_client:enqueue(m1, F0),
                  {ok, F2} = rabbit_fifo_client:enqueue(m2, F1),
                  process_ra_events(F2, 100),
                  receive stop ->  ok end
          end),
    F0 = rabbit_fifo_client:init(ClusterName, [ServerId], 4),
    {ok, _} = rabbit_fifo_client:checkout(<<"tag">>, 1, #{}, F0),
    {ok, {_, Ready}, _} = ra:local_query(ServerId,
                                         fun rabbit_fifo:query_messages_ready/1),
    ?assertEqual(1, Ready),
    {ok, {_, Checked}, _} = ra:local_query(ServerId,
                                           fun rabbit_fifo:query_messages_checked_out/1),
    ?assertEqual(1, Checked),
    {ok, {_, Processes}, _} = ra:local_query(ServerId,
                                             fun rabbit_fifo:query_processes/1),
    ?assertEqual(2, length(Processes)),
    P !  stop,
    ra:stop_server(ServerId),
    ok.

dead_letter_handler(Pid, Msgs) ->
    Pid ! {dead_letter, Msgs}.

dequeue(Config) ->
    ClusterName = ?config(cluster_name, Config),
    ServerId = ?config(node_id, Config),
    UId = ?config(uid, Config),
    Tag = UId,
    ok = start_cluster(ClusterName, [ServerId]),
    F1 = rabbit_fifo_client:init(ClusterName, [ServerId]),
    {empty, F1b} = rabbit_fifo_client:dequeue(Tag, settled, F1),
    {ok, F2_} = rabbit_fifo_client:enqueue(msg1, F1b),
    {_, _, F2} = process_ra_events(F2_, 100),

    % {ok, {{0, {_, msg1}}, _}, F3} = rabbit_fifo_client:dequeue(Tag, settled, F2),
    {ok, _, {_, _, 0, _, msg1}, F3} = rabbit_fifo_client:dequeue(Tag, settled, F2),
    {ok, F4_} = rabbit_fifo_client:enqueue(msg2, F3),
    {_, _, F4} = process_ra_events(F4_, 100),
    % {ok, {{MsgId, {_, msg2}}, _}, F5} = rabbit_fifo_client:dequeue(Tag, unsettled, F4),
    {ok, _, {_, _, MsgId, _, msg2}, F5} = rabbit_fifo_client:dequeue(Tag, settled, F4),
    _F6 = rabbit_fifo_client:settle(Tag, [MsgId], F5),
    ra:stop_server(ServerId),
    ok.

enq_deq_n(N, F0) ->
    enq_deq_n(N, F0, []).

enq_deq_n(0, F0, Acc) ->
    {_, _, F} = process_ra_events(F0, 100),
    {F, Acc};
enq_deq_n(N, F, Acc) ->
    {ok, F1} = rabbit_fifo_client:enqueue(N, F),
    {_, _, F2} = process_ra_events(F1, 10),
    {ok, _, {_, _, _, _, Deq}, F3} = rabbit_fifo_client:dequeue(term_to_binary(N), settled, F2),
    % {ok, {{_, {_, Deq}}, _}, F3} = rabbit_fifo_client:dequeue(term_to_binary(N), settled, F2),
    {_, _, F4} = process_ra_events(F3, 5),
    enq_deq_n(N-1, F4, [Deq | Acc]).

conf(ClusterName, UId, ServerId, _, Peers) ->
    #{cluster_name => ClusterName,
      id => ServerId,
      uid => UId,
      log_init_args => #{uid => UId},
      initial_members => Peers,
      machine => {module, rabbit_fifo, #{}}}.

process_ra_event(State, Wait) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("processed ra event ~p~n", [Evt]),
            {ok, S, _Actions} =
                rabbit_fifo_client:handle_ra_event(From, Evt, State),
            S
    after Wait ->
              exit(ra_event_timeout)
    end.

process_ra_events(State0, Wait) ->
    process_ra_events(State0, [], Wait).

process_ra_events(State, Acc, Wait) ->
    DeliveryFun = fun ({deliver, _, Tag, Msgs}, S) ->
                          MsgIds = [element(1, M) || M <- Msgs],
                          rabbit_fifo_client:settle(Tag, MsgIds, S)
                  end,
    process_ra_events0(State, Acc, [], Wait, DeliveryFun).

process_ra_events0(State0, Acc, Actions0, Wait, DeliveryFun) ->
    receive
        {ra_event, From, Evt} ->
            ct:pal("Ra event ~w", [Evt]),
            case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
                {ok, State1, Actions1} ->
                    {Msgs, Actions, State} =
                        lists:foldl(
                          fun ({deliver, _, _, Msgs} = Del, {M, A, S}) ->
                                  {M ++ Msgs, A, DeliveryFun(Del, S)};
                              (Ac, {M, A, S}) ->
                                  {M, A ++ [Ac], S}
                          end, {Acc, [], State1}, Actions1),
                    process_ra_events0(State, Msgs, Actions0 ++ Actions,
                                       Wait, DeliveryFun);
                eol ->
                    eol
            end
    after Wait ->
              {Acc, Actions0, State0}
    end.

discard_next_delivery(State0, Wait) ->
    element(3,
            process_ra_events0(State0, [], [], Wait,
                               fun ({deliver, Tag, _, Msgs}, S) ->
                                       MsgIds = [element(3, M) || M <- Msgs],
                                       ct:pal("discarding ~w", [MsgIds]),
                                       rabbit_fifo_client:discard(Tag, MsgIds, S)
                               end)).

return_next_delivery(State0, Wait) ->
    element(3,
            process_ra_events0(State0, [], [], Wait,
                               fun ({deliver, Tag, _, Msgs}, S) ->
                                       MsgIds = [element(3, M) || M <- Msgs],
                                       rabbit_fifo_client:return(Tag, MsgIds, S)
                               end)).
    % receive
    %     {ra_event, From, Evt} ->
    %         case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
    %             {internal, _, _, State} ->
    %                 return_next_delivery(State, Wait);
    %             {{delivery, Tag, Msgs}, State1} ->
    %                 MsgIds = [element(1, M) || M <- Msgs],
    %                 {ok, State} = rabbit_fifo_client:return(Tag, MsgIds,
    %                                                     State1),
    %                 State
    %         end
    % after Wait ->
    %           State0
    % end.

validate_process_down(Name, 0) ->
    exit({process_not_down, Name});
validate_process_down(Name, Num) ->
    case whereis(Name) of
        undefined ->
            ok;
        _ ->
            timer:sleep(100),
            validate_process_down(Name, Num-1)
    end.

start_cluster(ClusterName, ServerIds, RaFifoConfig) ->
    {ok, Started, _} = ra:start_cluster(ClusterName#resource.name,
                                        {module, rabbit_fifo, RaFifoConfig},
                                        ServerIds),
    ?assertEqual(length(Started), length(ServerIds)),
    ok.

start_cluster(ClusterName, ServerIds) ->
    start_cluster(ClusterName, ServerIds, #{name => some_name,
                                            queue_resource => ClusterName}).

flush() ->
    receive
        Msg ->
            ct:pal("flushed: ~w~n", [Msg]),
            flush()
    after 10 ->
              ok
    end.
