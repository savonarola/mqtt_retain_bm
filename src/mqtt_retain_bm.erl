-module(mqtt_retain_bm).

-export([main/1]).

-define(OPTS, [
    {hosts, $h, "hosts", {string, "localhost"},
        "mqtt server hostname or comma-separated hostnames"},
    {port, $p, "port", {integer, 1883}, "mqtt server port number"},
    {clientid_prefix, undefined, "clientid_prefix", {string, "mqtt_retain_bm"},
        "mqtt clientid prefix"},
    {clients, $c, "clients", {integer, 1}, "number of mqtt clients"},
    {topic, $t, "topic", {string, "config/{docid}/{partid}"},
        "mqtt topic pattern, {docid} and {partid} will be substituted"},
    {docid_start, undefined, "docid_start", {integer, 1}, "start of docid range"},
    {docid_end, undefined, "docid_end", {integer, 100}, "end of docid range"},
    {partid_start, undefined, "partid_start", {integer, 1}, "start of partid range"},
    {partid_end, undefined, "partid_end", {integer, 16}, "end of partid range"},
    {client_pps, undefined, "client_pps", {integer, 1000}, "publish rate per client"},
    {payload_size, undefined, "payload_size", {integer, 8192}, "payload size in bytes"},
    {timeout, undefined, "timeout", {integer, 5000}, "publish timeout in milliseconds"},
    {start_interval, undefined, "start_interval", {integer, 50}, "client start interval in milliseconds"},
    {help, $?, "help", {boolean, false}, "display this help message"}
]).

main(Args) ->
    case getopt:parse(?OPTS, Args) of
        {ok, {Opts, _}} ->
            case proplists:get_value(help, Opts) of
                true ->
                    getopt:usage(?OPTS, atom_to_list(?MODULE));
                false ->
                    run(Opts)
            end,
            halt(0);
        {error, Reason} ->
            io:format("error: ~p~n", [Reason]),
            getopt:usage(?OPTS, atom_to_list(?MODULE)),
            halt(1)
    end.

run(Opts) ->
    Pid = start_provider(Opts),

    Consumers = lists:map(
        fun(N) ->
            ok = io:format("starting consumer ~p~n", [N]),
            Consumer = start_consumer([{provider_pid, Pid}, {n, N} | Opts]),
            ok = io:format("started consumer ~p~n", [N]),
            ok = timer:sleep(proplists:get_value(start_interval, Opts)),
            Consumer
        end,
        lists:seq(1, proplists:get_value(clients, Opts))
    ),
    wait(Consumers),
    halt(0).

%%--------------------------------------------------------------------
%% Provider
%%--------------------------------------------------------------------

start_provider(Opts) ->
    DocIdStart = proplists:get_value(docid_start, Opts),
    DocIdEnd = proplists:get_value(docid_end, Opts),
    spawn_link(
        fun() ->
            loop_docs(Opts, DocIdStart, DocIdEnd)
        end
    ).

loop_docs(Opts, DocId, DocIdEnd) when DocId =< DocIdEnd ->
    PartIdStart = proplists:get_value(partid_start, Opts),
    PartIdEnd = proplists:get_value(partid_end, Opts),
    loop_parts(Opts, DocId, DocIdEnd, PartIdStart, PartIdEnd);
loop_docs(_Opts, _DocId, _DocIdEnd) ->
    loop_docs_finished().

loop_docs_finished() ->
    receive
        {get, Pid, Ref} ->
            Pid ! {Ref, completed}
    end,
    loop_docs_finished().

loop_parts(Opts, DocId, DocIdEnd, PartId, PartIdEnd) when PartId =< PartIdEnd ->
    receive
        {get, Pid, Ref} ->
            Pid ! {Ref, {DocId, PartId}}
    end,
    loop_parts(Opts, DocId, DocIdEnd, PartId + 1, PartIdEnd);
loop_parts(Opts, DocId, DocIdEnd, _PartIdStart, _PartIdEnd) ->
    loop_docs(Opts, DocId + 1, DocIdEnd).

%%--------------------------------------------------------------------
%% Consumers
%%--------------------------------------------------------------------

start_consumer(Opts) ->
    OwnerPid = self(),
    spawn_monitor(
        fun() ->
            try
                Conn = connect(Opts),
                consume([{conn, Conn}, {owner_pid, OwnerPid} | Opts], 0),
                emqtt:disconnect(Conn)
            catch
                Class:Error:Stack ->
                    io:format("consumer failed: ~p: ~p~n~p", [Class, Error, Stack]),
                    erlang:raise(Class, Error, Stack)
            end
        end
    ).

connect(Opts) ->
    Host = host(Opts),
    Port = proplists:get_value(port, Opts),
    ClientId = clientid(Opts),
    io:format("starting client ~p: ~p: ~p~n", [Host, Port, ClientId]),
    {ok, Conn} = emqtt:start_link([{host, Host}, {port, Port}, {clientid, ClientId}]),
    io:format("started client ~p: ~p: ~p~n", [Host, Port, ClientId]),
    {ok, _Result} = emqtt:connect(Conn),
    io:format("connected to ~p:~p as ~p~n", [Host, Port, ClientId]),
    Conn.

host(Opts) ->
    Hosts = proplists:get_value(hosts, Opts),
    HostList = string:tokens(Hosts, ","),
    lists:nth(rand:uniform(length(HostList)), HostList).

clientid(Opts) ->
    Prefix = proplists:get_value(clientid_prefix, Opts),
    Prefix ++ "_" ++ integer_to_list(erlang:unique_integer([positive])).

consume(Opts, TotalCount) ->
    StartTime = erlang:system_time(millisecond),
    Count = send_next(Opts),
    N = proplists:get_value(n, Opts),
    case Count of
        0 ->
            io:format("[~p] sent ~p total messages~n", [N, TotalCount]),
            ok;
        _ ->
            wait_ack(Count),
            EndTime = erlang:system_time(millisecond),
            Elapsed = EndTime - StartTime,
            io:format("[~p] sent ~p messages in ~p ms, ~p total~n", [
                N, Count, Elapsed, TotalCount + Count
            ]),
            sleep(1000 - Elapsed),
            consume(Opts, TotalCount + Count)
    end.

sleep(Ms) when Ms > 0 ->
    timer:sleep(Ms);
sleep(_Ms) ->
    ok.

wait_ack(0) ->
    ok;
wait_ack(N) ->
    receive
        {ack, _MsgId} ->
            wait_ack(N - 1)
    end.

send_next(Opts) ->
    ClientPPS = proplists:get_value(client_pps, Opts),
    send_next(Opts, ClientPPS, 0).

send_next(_Opts, 0, Count) ->
    Count;
send_next(Opts, NRemained, Count) ->
    case request(proplists:get_value(provider_pid, Opts)) of
        completed ->
            Count;
        {DocId, PartId} ->
            Topic = format_topic(Opts, DocId, PartId),
            Payload = payload(Opts),
            Self = self(),
            Callback = fun(Result) -> Self ! {ack, Result} end,
            Conn = proplists:get_value(conn, Opts),
            Timeout = proplists:get_value(timeout, Opts),
            _ = emqtt:publish_async(
                Conn, Topic, _Props = #{}, Payload, [{qos, 1}, {retain, true}], Timeout, Callback
            ),
            send_next(Opts, NRemained - 1, Count + 1)
    end.

payload(Opts) ->
    PayloadSize = proplists:get_value(payload_size, Opts),
    rand:bytes(PayloadSize).

format_topic(Opts, DocId, PartId) ->
    Topic0 = proplists:get_value(topic, Opts),
    Topic1 = string:replace(Topic0, "{docid}", integer_to_list(DocId)),
    Topic2 = string:replace(Topic1, "{partid}", integer_to_list(PartId)),
    iolist_to_binary(Topic2).

request(Pid) ->
    Ref = make_ref(),
    Pid ! {get, self(), Ref},
    receive
        {Ref, {DocId, PartId}} ->
            {DocId, PartId};
        {Ref, completed} ->
            completed
    end.

wait(Consumers) ->
    lists:foreach(
        fun({Pid, Ref}) ->
            receive
                {'DOWN', Ref, process, Pid, Reason} ->
                    io:format("consumer finished: ~p~n", [Reason]),
                    ok
            end
        end,
        Consumers
    ).
