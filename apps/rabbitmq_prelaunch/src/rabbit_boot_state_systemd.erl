%%%-------------------------------------------------------------------
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
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_boot_state_systemd).

-behavior(gen_server).

-export([start_link/0]).

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {sd_notify_module,
                socket}).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    case code:load_file(sd_notify) of
        {module, sd_notify} ->
            {ok, #state{sd_notify_module = sd_notify}};
        {error, _} ->
            case rabbit_prelaunch:get_context() of
                #{systemd_notify_socket := Socket} when Socket =/= undefined ->
                    {ok, #state{socket = Socket}};
                _ ->
                    ignore
            end
    end.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({notify_boot_state, ready}, #{sd_notify_module := SDNotify} = State) ->
    rabbit_log_prelaunch:debug("notifying systemd of readiness via native module"),
    sd_notify_legacy(SDNotify),
    {noreply, State};
handle_cast({notify_boot_state, ready}, #{socket := Socket} = State) ->
    rabbit_log_prelaunch:debug("notifying systemd of readiness via socat"),
    sd_notify_socat(Socket),
    {noreply, State};
handle_cast({notify_boot_state, _BootState}, State) ->
    {noreply, State}.

handle_info(Msg, State) ->
    io:format(standard_error, "Unexpected message: ~p~n",[Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format(standard_error, "~p terminating.~n", [?MODULE]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%% Private

sd_notify_message() ->
  "READY=1\nSTATUS=Initialized\nMAINPID=" ++ os:getpid() ++ "\n".

sd_notify_legacy(SDNotify) ->
    SDNotify:sd_notify(0, sd_notify_message()).

%% socat(1) is the most portable way the sd_notify could be
%% implemented in erlang, without introducing some NIF. Currently the
%% following issues prevent us from implementing it in a more
%% reasonable way:
%% - systemd-notify(1) is unstable for non-root users
%% - erlang doesn't support unix domain sockets.
%%
%% Some details on how we ended with such a solution:
%%   https://github.com/rabbitmq/rabbitmq-server/issues/664
sd_notify_socat(Socket) ->
  case sd_current_unit() of
    {ok, Unit} ->
      io:format(standard_error, "systemd unit for activation check: \"~s\"~n", [Unit]),
      sd_notify_socat(Socket, Unit);
    _ ->
      false
  end.

sd_notify_socat(Socket, Unit) ->
  try sd_open_port(Socket) of
    Port ->
      Port ! {self(), {command, sd_notify_message()}},
      Result = sd_wait_activation(Port, Unit),
      port_close(Port),
      Result
  catch
    Class:Reason ->
      io:format(standard_error, "Failed to start socat ~p:~p~n", [Class, Reason]),
      false
  end.

sd_current_unit() ->
  CmdOut = os:cmd("ps -o unit= -p " ++ os:getpid()),
  case catch re:run(CmdOut, "([-.@0-9a-zA-Z]+)", [unicode, {capture, all_but_first, list}]) of
    {'EXIT', _} ->
      error;
    {match, [Unit]} ->
      {ok, Unit};
    _ ->
      error
  end.

socat_socket_arg("@" ++ AbstractUnixSocket) ->
  "abstract-sendto:" ++ AbstractUnixSocket;
socat_socket_arg(UnixSocket) ->
  "unix-sendto:" ++ UnixSocket.

sd_open_port(Socket) ->
  open_port(
    {spawn_executable, os:find_executable("socat")},
    [{args, [socat_socket_arg(Socket), "STDIO"]},
      use_stdio, out]).

sd_wait_activation(Port, Unit) ->
  case os:find_executable("systemctl") of
    false ->
      io:format(standard_error, "'systemctl' unavailable, falling back to sleep~n", []),
      timer:sleep(5000),
      true;
    _ ->
      sd_wait_activation(Port, Unit, 10)
  end.

sd_wait_activation(_, _, 0) ->
  io:format(standard_error, "Service still in 'activating' state, bailing out~n", []),
  false;
sd_wait_activation(Port, Unit, AttemptsLeft) ->
  case os:cmd("systemctl show --property=ActiveState -- '" ++ Unit ++ "'") of
    "ActiveState=activating\n" ->
      timer:sleep(1000),
      sd_wait_activation(Port, Unit, AttemptsLeft - 1);
    "ActiveState=" ++ _ ->
      true;
    _ = Err->
      io:format(standard_error, "Unexpected status from systemd ~p~n", [Err]),
      false
  end.
