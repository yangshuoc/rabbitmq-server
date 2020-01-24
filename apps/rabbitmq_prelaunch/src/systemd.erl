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

-module(systemd).

-export([notify_ready/0]).

-spec notify_ready() -> boolean().
%% Try to send systemd ready notification. standard_error is used
%% intentionally in all logging statements, so all this messages will
%% end up in systemd journal.
notify_ready() ->
  case rabbit_prelaunch:get_context() of
    #{systemd_notify_socket := Socket} when Socket =/= undefined ->
      Notified = sd_notify_legacy() orelse sd_notify_socat(),
      if not Notified ->
        io:format(standard_error, "systemd READY notification failed, beware of timeouts~n", []) end,
      Notified;
    _ -> rabbit_log_prelaunch:debug("NOTIFY_SOCKET has not been set, will not attempt to notify systemd~n", []), false
  end.

sd_notify_message() ->
  "READY=1\nSTATUS=Initialized\nMAINPID=" ++ os:getpid() ++ "\n".

sd_notify_legacy() ->
  case code:load_file(sd_notify) of
    {module, sd_notify} ->
      SDNotify = sd_notify,
      SDNotify:sd_notify(0, sd_notify_message()),
      true;
    {error, _} ->
      false
  end.

%% socat(1) is the most portable way the sd_notify could be
%% implemented in erlang, without introducing some NIF. Currently the
%% following issues prevent us from implementing it in a more
%% reasonable way:
%% - systemd-notify(1) is unstable for non-root users
%% - erlang doesn't support unix domain sockets.
%%
%% Some details on how we ended with such a solution:
%%   https://github.com/rabbitmq/rabbitmq-server/issues/664
sd_notify_socat() ->
  case sd_current_unit() of
    {ok, Unit} ->
      io:format(standard_error, "systemd unit for activation check: \"~s\"~n", [Unit]),
      sd_notify_socat(Unit);
    _ ->
      false
  end.

sd_notify_socat(Unit) ->
  try sd_open_port() of
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

sd_open_port() ->
  #{systemd_notify_socket := Socket} = rabbit_prelaunch:get_context(),
  true = Socket =/= undefined,
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
