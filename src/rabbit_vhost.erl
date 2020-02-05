%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_vhost).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("vhost.hrl").

-export([recover/0, recover/1]).
-export([add/2, add/4, delete/2, exists/1, with/2, with_user_and_vhost/3, assert/1, update/2,
         set_limits/2, vhost_cluster_state/1, is_running_on_all_nodes/1, await_running_on_all_nodes/2,
        list/0, count/0, list_names/0, all/0, parse_tags/1]).
-export([info/1, info/2, info_all/0, info_all/1, info_all/2, info_all/3]).
-export([dir/1, msg_store_dir_path/1, msg_store_dir_wildcard/0]).
-export([delete_storage/1]).
-export([vhost_down/1]).

%%
%% API
%%

recover() ->
    %% Clear out remnants of old incarnation, in case we restarted
    %% faster than other nodes handled DOWN messages from us.
    rabbit_amqqueue:on_node_down(node()),

    rabbit_amqqueue:warn_file_limit(),

    %% Prepare rabbit_semi_durable_route table
    rabbit_binding:recover(),

    %% rabbit_vhost_sup_sup will start the actual recovery.
    %% So recovery will be run every time a vhost supervisor is restarted.
    ok = rabbit_vhost_sup_sup:start(),

    [ok = rabbit_vhost_sup_sup:init_vhost(VHost) || VHost <- list_names()],
    ok.

recover(VHost) ->
    VHostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Making sure data directory '~ts' for vhost '~s' exists~n",
                    [VHostDir, VHost]),
    VHostStubFile = filename:join(VHostDir, ".vhost"),
    ok = rabbit_file:ensure_dir(VHostStubFile),
    ok = file:write_file(VHostStubFile, VHost),
    {Recovered, Failed} = rabbit_amqqueue:recover(VHost),
    AllQs = Recovered ++ Failed,
    QNames = [amqqueue:get_name(Q) || Q <- AllQs],
    ok = rabbit_binding:recover(rabbit_exchange:recover(VHost), QNames),
    ok = rabbit_amqqueue:start(Recovered),
    %% Start queue mirrors.
    ok = rabbit_mirror_queue_misc:on_vhost_up(VHost),
    ok.

-define(INFO_KEYS, vhost:info_keys()).

-spec parse_tags(binary() | string() | atom()) -> [atom()].
parse_tags(undefined) ->
    [];
parse_tags("") ->
    [];
parse_tags(<<"">>) ->
    [];
parse_tags(Val) when is_binary(Val) ->
    parse_tags(rabbit_data_coercion:to_list(Val));
parse_tags(Val) when is_list(Val) ->
    [trim_tag(Tag) || Tag <- re:split(Val, ",", [{return, list}])].

-spec add(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(VHost, ActingUser) ->
    case exists(VHost) of
        true  -> ok;
        false -> do_add(VHost, <<"">>, [], ActingUser)
    end.

-spec add(vhost:name(), binary(), [atom()], rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

add(Name, Description, Tags, ActingUser) ->
    case exists(Name) of
        true  -> ok;
        false -> do_add(Name, Description, Tags, ActingUser)
    end.

do_add(Name, Description, Tags, ActingUser) ->
    rabbit_log:info("Adding vhost '~s' (description: '~s')", [Name, Description]),
    VHost = rabbit_misc:execute_mnesia_transaction(
          fun () ->
                  case mnesia:wread({rabbit_vhost, Name}) of
                      [] ->
                        Row = vhost:new(Name, [], #{description => Description, tags => Tags}),
                        rabbit_log:debug("Inserting a virtual host record ~p", [Row]),
                        ok = mnesia:write(rabbit_vhost, Row, write),
                        Row;
                      %% the vhost already exists
                      [Row] ->
                        Row
                  end
          end,
          fun (VHost1, true) ->
                  VHost1;
              (VHost1, false) ->
                  [begin
                    Resource = rabbit_misc:r(Name, exchange, ExchangeName),
                    rabbit_log:debug("Will declare an exchange ~p", [Resource]),
                    _ = rabbit_exchange:declare(Resource, Type, true, false, Internal, [], ActingUser)
                  end || {ExchangeName, Type, Internal} <-
                          [{<<"">>,                   direct,  false},
                           {<<"amq.direct">>,         direct,  false},
                           {<<"amq.topic">>,          topic,   false},
                           %% per 0-9-1 pdf
                           {<<"amq.match">>,          headers, false},
                           %% per 0-9-1 xml
                           {<<"amq.headers">>,        headers, false},
                           {<<"amq.fanout">>,         fanout,  false},
                           {<<"amq.rabbitmq.trace">>, topic,   true}]],
                  VHost1
          end),
    case rabbit_vhost_sup_sup:start_on_all_nodes(Name) of
        ok ->
            rabbit_event:notify(vhost_created, info(VHost)
                                ++ [{user_who_performed_action, ActingUser},
                                    {description, Description},
                                    {tags, Tags}]),
            ok;
        {error, Reason} ->
            Msg = rabbit_misc:format("failed to set up vhost '~s': ~p",
                                     [Name, Reason]),
            {error, Msg}
    end.

-spec delete(vhost:name(), rabbit_types:username()) -> rabbit_types:ok_or_error(any()).

delete(VHost, ActingUser) ->
    %% FIXME: We are forced to delete the queues and exchanges outside
    %% the TX below. Queue deletion involves sending messages to the queue
    %% process, which in turn results in further mnesia actions and
    %% eventually the termination of that process. Exchange deletion causes
    %% notifications which must be sent outside the TX
    rabbit_log:info("Deleting vhost '~s'~n", [VHost]),
    QDelFun = fun (Q) -> rabbit_amqqueue:delete(Q, false, false, ActingUser) end,
    [begin
         Name = amqqueue:get_name(Q),
         assert_benign(rabbit_amqqueue:with(Name, QDelFun), ActingUser)
     end || Q <- rabbit_amqqueue:list(VHost)],
    [assert_benign(rabbit_exchange:delete(Name, false, ActingUser), ActingUser) ||
        #exchange{name = Name} <- rabbit_exchange:list(VHost)],
    Funs = rabbit_misc:execute_mnesia_transaction(
          with(VHost, fun () -> internal_delete(VHost, ActingUser) end)),
    ok = rabbit_event:notify(vhost_deleted, [{name, VHost},
                                             {user_who_performed_action, ActingUser}]),
    [case Fun() of
         ok                                  -> ok;
         {error, {no_such_vhost, VHost}} -> ok
     end || Fun <- Funs],
    %% After vhost was deleted from mnesia DB, we try to stop vhost supervisors
    %% on all the nodes.
    rabbit_vhost_sup_sup:delete_on_all_nodes(VHost),
    ok.

%% 50 ms
-define(AWAIT_SAMPLE_INTERVAL, 50).

-spec await_running_on_all_nodes(vhost:name(), integer()) -> ok | {error, timeout}.
await_running_on_all_nodes(VHost, Timeout) ->
    Attempts = round(Timeout / ?AWAIT_SAMPLE_INTERVAL),
    await_running_on_all_nodes0(VHost, Attempts).

await_running_on_all_nodes0(_VHost, 0) ->
    {error, timeout};
await_running_on_all_nodes0(VHost, Attempts) ->
    case is_running_on_all_nodes(VHost) of
        true  -> ok;
        _     ->
            timer:sleep(?AWAIT_SAMPLE_INTERVAL),
            await_running_on_all_nodes0(VHost, Attempts - 1)
    end.

-spec is_running_on_all_nodes(vhost:name()) -> boolean().
is_running_on_all_nodes(VHost) ->
    States = vhost_cluster_state(VHost),
    lists:all(fun ({_Node, State}) -> State =:= running end,
              States).

-spec vhost_cluster_state(vhost:name()) -> [{atom(), atom()}].
vhost_cluster_state(VHost) ->
    Nodes = rabbit_nodes:all_running(),
    lists:map(fun(Node) ->
        State = case rabbit_misc:rpc_call(Node,
                                          rabbit_vhost_sup_sup, is_vhost_alive,
                                          [VHost]) of
            {badrpc, nodedown} -> nodedown;
            true               -> running;
            false              -> stopped
        end,
        {Node, State}
    end,
    Nodes).

vhost_down(VHost) ->
    ok = rabbit_event:notify(vhost_down,
                             [{name, VHost},
                              {node, node()},
                              {user_who_performed_action, ?INTERNAL_USER}]).

delete_storage(VHost) ->
    VhostDir = msg_store_dir_path(VHost),
    rabbit_log:info("Deleting message store directory for vhost '~s' at '~s'~n", [VHost, VhostDir]),
    %% Message store should be closed when vhost supervisor is closed.
    case rabbit_file:recursive_delete([VhostDir]) of
        ok                   -> ok;
        {error, {_, enoent}} ->
            %% a concurrent delete did the job for us
            rabbit_log:warning("Tried to delete storage directories for vhost '~s', it failed with an ENOENT", [VHost]),
            ok;
        Other                ->
            rabbit_log:warning("Tried to delete storage directories for vhost '~s': ~p", [VHost, Other]),
            Other
    end.

assert_benign(ok, _)                 -> ok;
assert_benign({ok, _}, _)            -> ok;
assert_benign({ok, _, _}, _)         -> ok;
assert_benign({error, not_found}, _) -> ok;
assert_benign({error, {absent, Q, _}}, ActingUser) ->
    %% Removing the mnesia entries here is safe. If/when the down node
    %% restarts, it will clear out the on-disk storage of the queue.
    QName = amqqueue:get_name(Q),
    rabbit_amqqueue:internal_delete(QName, ActingUser).

internal_delete(VHost, ActingUser) ->
    [ok = rabbit_auth_backend_internal:clear_permissions(
            proplists:get_value(user, Info), VHost, ActingUser)
     || Info <- rabbit_auth_backend_internal:list_vhost_permissions(VHost)],
    TopicPermissions = rabbit_auth_backend_internal:list_vhost_topic_permissions(VHost),
    [ok = rabbit_auth_backend_internal:clear_topic_permissions(
        proplists:get_value(user, TopicPermission), VHost, ActingUser)
     || TopicPermission <- TopicPermissions],
    Fs1 = [rabbit_runtime_parameters:clear(VHost,
                                           proplists:get_value(component, Info),
                                           proplists:get_value(name, Info),
                                           ActingUser)
     || Info <- rabbit_runtime_parameters:list(VHost)],
    Fs2 = [rabbit_policy:delete(VHost, proplists:get_value(name, Info), ActingUser)
           || Info <- rabbit_policy:list(VHost)],
    ok = mnesia:delete({rabbit_vhost, VHost}),
    Fs1 ++ Fs2.

-spec exists(vhost:name()) -> boolean().

exists(VHost) ->
    mnesia:dirty_read({rabbit_vhost, VHost}) /= [].

-spec list_names() -> [vhost:name()].
list_names() -> mnesia:dirty_all_keys(rabbit_vhost).

%% Exists for backwards compatibility, prefer list_names/0.
-spec list() -> [vhost:name()].
list() -> list_names().

-spec all() -> [vhost:vhost()].
all() -> mnesia:dirty_match_object(rabbit_vhost, vhost:pattern_match_all()).

-spec count() -> non_neg_integer().
count() ->
    length(list()).

-spec with(vhost:name(), rabbit_misc:thunk(A)) -> A.

with(VHost, Thunk) ->
    fun () ->
            case mnesia:read({rabbit_vhost, VHost}) of
                [] ->
                    mnesia:abort({no_such_vhost, VHost});
                [_V] ->
                    Thunk()
            end
    end.

-spec with_user_and_vhost
        (rabbit_types:username(), vhost:name(), rabbit_misc:thunk(A)) -> A.

with_user_and_vhost(Username, VHost, Thunk) ->
    rabbit_misc:with_user(Username, with(VHost, Thunk)).

%% Like with/2 but outside an Mnesia tx

-spec assert(vhost:name()) -> 'ok'.

assert(VHost) -> case exists(VHost) of
                         true  -> ok;
                         false -> throw({error, {no_such_vhost, VHost}})
                     end.

-spec update(vhost:name(), fun((vhost:vhost()) -> vhost:vhost())) -> vhost:vhost().

update(VHost, Fun) ->
    case mnesia:read({rabbit_vhost, VHost}) of
        [] ->
            mnesia:abort({no_such_vhost, VHost});
        [V] ->
            V1 = Fun(V),
            ok = mnesia:write(rabbit_vhost, V1, write),
            V1
    end.

set_limits(VHost, undefined) ->
    vhost:set_limits(VHost, undefined);
set_limits(VHost, Limits) ->
    vhost:set_limits(VHost, Limits).


dir(Vhost) ->
    <<Num:128>> = erlang:md5(Vhost),
    rabbit_misc:format("~.36B", [Num]).

msg_store_dir_path(VHost) ->
    EncodedName = dir(VHost),
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), EncodedName])).

msg_store_dir_wildcard() ->
    rabbit_data_coercion:to_list(filename:join([msg_store_dir_base(), "*"])).

msg_store_dir_base() ->
    Dir = rabbit_mnesia:dir(),
    filename:join([Dir, "msg_stores", "vhosts"]).

-spec trim_tag(list() | binary() | atom()) -> atom().
trim_tag(Val) ->
    rabbit_data_coercion:to_atom(string:trim(rabbit_data_coercion:to_list(Val))).

%%----------------------------------------------------------------------------

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,    VHost) -> vhost:get_name(VHost);
i(tracing, VHost) -> rabbit_trace:enabled(vhost:get_name(VHost));
i(cluster_state, VHost) -> vhost_cluster_state(vhost:get_name(VHost));
i(description, VHost) -> vhost:get_description(VHost);
i(tags, VHost) -> vhost:get_tags(VHost);
i(metadata, VHost) -> vhost:get_metadata(VHost);
i(Item, VHost)     ->
  rabbit_log:error("Don't know how to compute a virtual host info item '~s' for virtual host '~p'", [Item, VHost]),
  throw({bad_argument, Item}).

-spec info(vhost:vhost() | vhost:name()) -> rabbit_types:infos().

info(VHost) when ?is_vhost(VHost) ->
    infos(?INFO_KEYS, VHost);
info(Key) ->
    case mnesia:dirty_read({rabbit_vhost, Key}) of
        [] -> [];
        [VHost] -> infos(?INFO_KEYS, VHost)
    end.

-spec info(vhost:vhost(), rabbit_types:info_keys()) -> rabbit_types:infos().
info(VHost, Items) -> infos(Items, VHost).

-spec info_all() -> [rabbit_types:infos()].
info_all()       -> info_all(?INFO_KEYS).

-spec info_all(rabbit_types:info_keys()) -> [rabbit_types:infos()].
info_all(Items) -> [info(VHost, Items) || VHost <- all()].

info_all(Ref, AggregatorPid)        -> info_all(?INFO_KEYS, Ref, AggregatorPid).

-spec info_all(rabbit_types:info_keys(), reference(), pid()) -> 'ok'.
info_all(Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
       AggregatorPid, Ref, fun(VHost) -> info(VHost, Items) end, all()).
