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
    {pps, undefined, "pps", {integer, 20000}, "publish rate"},
    {payload_size, undefined, "payload_size", {integer, 8192}, "payload size in bytes"},
    {start_interval, undefined, "start_interval", {integer, 50},
        "client start interval in milliseconds"},
    {ifaddrs, undefined, "ifaddrs", {string, ""},
        "network interfaces address to bind"},
    {lowmem, undefined, "lowmem", {boolean, false}, "use low memory mode"},
    {help, $?, "help", {boolean, false}, "display this help message"}
]).

main(Args) ->
    case parse_opts(Args) of
        {ok, Opts} ->
            case maps:get(help, Opts) of
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

parse_opts(Args) ->
    case getopt:parse(?OPTS, Args) of
        {ok, {Opts0, _}} ->
            Opts1 = maps:from_list(Opts0),
            Opts2 = parse_ifaddrs(Opts1),
            Opts3 = parse_hosts(Opts2),
            {ok, Opts3};
        {error, Reason} ->
            io:format("error: ~p~n", [Reason]),
            getopt:usage(?OPTS, atom_to_list(?MODULE)),
            halt(1)
    end.

parse_ifaddrs(#{ifaddrs := ""} = Opts) -> Opts;
parse_ifaddrs(#{ifaddrs := IfAddrs} = Opts) ->
    IfAddrList0 = string:tokens(IfAddrs, ","),
    IfAddrList1 = lists:map(
        fun(IfAddr) ->
            case inet_parse:address(IfAddr) of
                {ok, IpAddr} ->
                    IpAddr;
                {error, Reason} ->
                    io:format("bad ifaddr: ~p, error: ~p~n", [IfAddr, Reason]),
                    getopt:usage(?OPTS, atom_to_list(?MODULE)),
                    halt(1)
            end
        end,
        IfAddrList0
    ),
    Opts#{ifaddrs => IfAddrList1}.

parse_hosts(Opts) ->
    Hosts0 = maps:get(hosts, Opts),
    Hosts1 = string:tokens(Hosts0, ","),
    Opts#{hosts => Hosts1}.

run(Opts) ->
    Pid = start_provider(Opts),

    Consumers = lists:map(
        fun(N) ->
            ok = io:format("starting consumer ~p~n", [N]),
            Consumer = start_consumer(Opts#{n => N, provider_pid => Pid}),
            ok = io:format("started consumer ~p~n", [N]),
            ok = timer:sleep(maps:get(start_interval, Opts)),
            Consumer
        end,
        lists:seq(1, maps:get(clients, Opts))
    ),
    wait(Consumers),
    halt(0).

%%--------------------------------------------------------------------
%% Provider
%%--------------------------------------------------------------------

start_provider(Opts) ->
    DocIdStart = maps:get(docid_start, Opts),
    DocIdEnd = maps:get(docid_end, Opts),
    spawn_link(
        fun() ->
            Ctx = new_ctx(Opts),
            loop_docs(Ctx, Opts, DocIdStart, DocIdEnd)
        end
    ).

loop_docs(Ctx, Opts, DocId, DocIdEnd) when DocId =< DocIdEnd ->
    PartIdStart = maps:get(partid_start, Opts),
    PartIdEnd = maps:get(partid_end, Opts),
    loop_parts(Ctx, Opts, DocId, DocIdEnd, PartIdStart, PartIdEnd);
loop_docs(#{
    start_time := StartTime, total_count := TotalCount, done_count := DoneCount
}, _Opts, _DocId, _DocIdEnd) ->
    CurrentTime = erlang:system_time(millisecond),
    TotalElapsedTimeSec = (CurrentTime - StartTime) div 1000,
    io:format("Finished ~p of ~p in ~ps~n", [
        DoneCount, TotalCount, TotalElapsedTimeSec
    ]),
    loop_docs_finished().

loop_docs_finished() ->
    receive
        {get, Pid, Ref} ->
            Pid ! {Ref, completed}
    end,
    loop_docs_finished().

loop_parts(Ctx0, Opts, DocId, DocIdEnd, PartId, PartIdEnd) when PartId =< PartIdEnd ->
    receive
        {get, Pid, Ref} ->
            Pid ! {Ref, {DocId, PartId}}
    end,
    Ctx1 = update_ctx(Opts, Ctx0),
    loop_parts(Ctx1, Opts, DocId, DocIdEnd, PartId + 1, PartIdEnd);
loop_parts(Ctx, Opts, DocId, DocIdEnd, _PartIdStart, _PartIdEnd) ->
    loop_docs(Ctx, Opts, DocId + 1, DocIdEnd).

update_ctx(Opts, #{
    start_time := StartTime, period_start_time := PeriodStartTime, total_count := TotalCount,
    period_left_count := PeriodLeftCount, done_count := DoneCount0
} = Ctx) when PeriodLeftCount =< 1 ->
    CurrentTime = erlang:system_time(millisecond),
    PeriondElapsedTime = CurrentTime - PeriodStartTime,
    TotalElapsedTimeSec = (CurrentTime - StartTime) div 1000,
    DoneCount1 = DoneCount0 + 1,
    PPS = maps:get(pps, Opts),
    io:format("[~p] Done ~p messages in ~pms; totally ~p of ~p in ~ps~n", [
        CurrentTime, PPS, PeriondElapsedTime, DoneCount1, TotalCount, TotalElapsedTimeSec
    ]),
    sleep(1000 - PeriondElapsedTime),
    Ctx#{
        period_start_time => erlang:system_time(millisecond),
        period_left_count => PPS,
        done_count => DoneCount1
    };
update_ctx(_Opts, #{period_left_count := PeriodLeftCount, done_count := DoneCount} = Ctx) ->
    Ctx#{period_left_count => PeriodLeftCount - 1, done_count => DoneCount + 1}.

new_ctx(Opts) ->
    #{
        start_time => erlang:system_time(millisecond),
        period_start_time => erlang:system_time(millisecond),
        total_count =>
            (maps:get(docid_end, Opts) - maps:get(docid_start, Opts) + 1) *
            (maps:get(partid_end, Opts) - maps:get(partid_start, Opts) + 1),
        period_left_count => maps:get(pps, Opts),
        done_count => 0
    }.

sleep(Milliseconds) when Milliseconds > 0 ->
    timer:sleep(Milliseconds);
sleep(_Milliseconds) ->
    ok.

%%--------------------------------------------------------------------
%% Consumers
%%--------------------------------------------------------------------

start_consumer(Opts) ->
    OwnerPid = self(),
    spawn_monitor(
        fun() ->
            try
                Conn = connect(Opts),
                consume(Opts#{owner_pid => OwnerPid, conn => Conn}, 0),
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
    Port = maps:get(port, Opts),
    ClientId = clientid(Opts),
    TCPOpts = tcp_opts(Opts),
    io:format("starting client ~p: ~p: ~p~n", [Host, Port, ClientId]),
    {ok, Conn} = emqtt:start_link([{host, Host}, {port, Port}, {clientid, ClientId}, {tcp_opts, TCPOpts}]),
    io:format("started client ~p: ~p: ~p~n", [Host, Port, ClientId]),
    {ok, _Result} = emqtt:connect(Conn),
    io:format("connected to ~p:~p as ~p~n", [Host, Port, ClientId]),
    Conn.

host(Opts) ->
    Hosts = maps:get(hosts, Opts),
    lists:nth(rand:uniform(length(Hosts)), Hosts).

tcp_opts(Opts) ->
    IpOpts = case Opts of
        #{ifaddrs := []} ->
            [];
        #{ifaddrs := IfAddrs} ->
            [{ip, lists:nth(rand:uniform(length(IfAddrs)), IfAddrs)}]
    end,
    BufOpts = case Opts of
        #{lowmem := true} ->
            [{recbuf, 64}, {sndbuf, 64}];
        _ ->
            []
    end,
    IpOpts ++ BufOpts.

clientid(Opts) ->
    Prefix = maps:get(clientid_prefix, Opts),
    Prefix ++ "_" ++ integer_to_list(erlang:unique_integer([positive])).

consume(Opts, TotalCount) ->
    case send_next(Opts) of
        completed ->
            N = maps:get(n, Opts),
            io:format("[~p] sent ~p messages~n", [N, TotalCount]),
            ok;
        _ ->
            consume(Opts, TotalCount + 1)
    end.

send_next(Opts) ->
    case request(maps:get(provider_pid, Opts)) of
        completed ->
            completed;
        {DocId, PartId} ->
            Topic = format_topic(Opts, DocId, PartId),
            Payload = payload(Opts),
            Conn = maps:get(conn, Opts),
            {ok, _} = emqtt:publish(Conn, Topic, _Props = #{}, Payload, [{qos, 1}, {retain, true}]),
            ok
    end.

payload(Opts) ->
    PayloadSize = maps:get(payload_size, Opts),
    rand:bytes(PayloadSize).

format_topic(Opts, DocId, PartId) ->
    Topic0 = maps:get(topic, Opts),
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
