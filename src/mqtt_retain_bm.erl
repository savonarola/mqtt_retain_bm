-module(mqtt_retain_bm).

-export([main/1, wake_up/0]).

-define(PUB_OPTS, [
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
    {ifaddrs, undefined, "ifaddrs", {string, ""}, "network interfaces address to bind"},
    {lowmem, undefined, "lowmem", {boolean, false}, "use low memory mode"},
    {help, $?, "help", {boolean, false}, "display this help message"}
]).

-define(SUB_OPTS, [
    {hosts, $h, "hosts", {string, "localhost"},
        "mqtt server hostname or comma-separated hostnames"},
    {port, $p, "port", {integer, 1883}, "mqtt server port number"},
    {clientid_prefix, undefined, "clientid_prefix", {string, "mqtt_retain_bm"},
        "mqtt clientid prefix"},
    {topic, $t, "topic", {string, "config/{docid}/{partid}"},
        "mqtt topic pattern, {docid} and {partid} will be substituted"},
    {docid_start, undefined, "docid_start", {integer, 1}, "start of docid range"},
    {docid_end, undefined, "docid_end", {integer, 100}, "end of docid range"},
    {partid_start, undefined, "partid_start", {integer, 1}, "start of partid range"},
    {partid_end, undefined, "partid_end", {integer, 16}, "end of partid range"},
    {duration, undefined, "duration", {integer, 15 * 60},
        "time to subscribe to all documents in seconds"},
    {part_receive_timeout, undefined, "part_receive_timeout", {integer, 16000},
        "timeout to receive all parts of a document in milliseconds"},
    {ifaddrs, undefined, "ifaddrs", {string, ""}, "network interfaces address to bind"},
    {lowmem, undefined, "lowmem", {boolean, false}, "use low memory mode"},
    {terminate_clients, undefined, "terminate_clients", {boolean, false},
        "terminate clients after all parts received"},
    {help, $?, "help", {boolean, false}, "display this help message"}
]).

main(["pub" | Args]) ->
    with_parsed_options(pub, ?PUB_OPTS, Args, fun run_pub/1);
main(["sub" | Args]) ->
    with_parsed_options(sub, ?SUB_OPTS, Args, fun run_sub/1);
main(_) ->
    io:format("usage: ~p [pub|sub]~n", [?MODULE]),
    halt(1).

with_parsed_options(Action, OptSpecs, Args, Fun) ->
    case parse_opts(Action, OptSpecs, Args) of
        {ok, Opts} ->
            case maps:get(help, Opts) of
                true ->
                    getopt:usage(OptSpecs, command_name(Action));
                false ->
                    Fun(Opts)
            end,
            halt(0);
        {error, Reason} ->
            io:format("error: ~p~n", [Reason]),
            getopt:usage(OptSpecs, command_name(Action)),
            halt(1)
    end.

parse_opts(Action, OptSpecs, Args) ->
    case getopt:parse(OptSpecs, Args) of
        {ok, {Opts0, _}} ->
            Opts1 = maps:from_list(Opts0),
            Opts2 = parse_ifaddrs(Action, OptSpecs, Opts1),
            Opts3 = parse_hosts(Opts2),
            {ok, Opts3};
        {error, Reason} ->
            io:format("error: ~p~n", [Reason]),
            getopt:usage(OptSpecs, command_name(Action)),
            halt(1)
    end.

command_name(Action) ->
    atom_to_list(Action) ++ " " ++ atom_to_list(?MODULE).

parse_ifaddrs(_Action, _OptSpecs, #{ifaddrs := ""} = Opts) ->
    Opts;
parse_ifaddrs(Action, OptSpecs, #{ifaddrs := IfAddrs} = Opts) ->
    IfAddrList0 = string:tokens(IfAddrs, ","),
    IfAddrList1 = lists:map(
        fun(IfAddr) ->
            case inet_parse:address(IfAddr) of
                {ok, IpAddr} ->
                    IpAddr;
                {error, Reason} ->
                    io:format("bad ifaddr: ~p, error: ~p~n", [IfAddr, Reason]),
                    getopt:usage(OptSpecs, atom_to_list(Action) ++ " " ++ atom_to_list(?MODULE)),
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

%%--------------------------------------------------------------------
%% Pub
%%--------------------------------------------------------------------

run_pub(Opts) ->
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
%% Pub producers
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
loop_docs(
    #{
        start_time := StartTime, total_count := TotalCount, done_count := DoneCount
    },
    _Opts,
    _DocId,
    _DocIdEnd
) ->
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

update_ctx(
    Opts,
    #{
        start_time := StartTime,
        period_start_time := PeriodStartTime,
        total_count := TotalCount,
        period_left_count := PeriodLeftCount,
        done_count := DoneCount0
    } = Ctx
) when PeriodLeftCount =< 1 ->
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
%% Pub consumers
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
    {ok, Conn} = emqtt:start_link([
        {host, Host}, {port, Port}, {clientid, ClientId}, {tcp_opts, TCPOpts}
    ]),
    io:format("started client ~p: ~p: ~p~n", [Host, Port, ClientId]),
    {ok, _Result} = emqtt:connect(Conn),
    io:format("connected to ~p:~p as ~p~n", [Host, Port, ClientId]),
    Conn.

host(Opts) ->
    Hosts = maps:get(hosts, Opts),
    lists:nth(rand:uniform(length(Hosts)), Hosts).

tcp_opts(Opts) ->
    IpOpts =
        case Opts of
            #{ifaddrs := []} ->
                [];
            #{ifaddrs := IfAddrs} ->
                [{ip, lists:nth(rand:uniform(length(IfAddrs)), IfAddrs)}]
        end,
    BufOpts =
        case Opts of
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

%%--------------------------------------------------------------------
%% Pub producers
%%--------------------------------------------------------------------

run_sub(Opts) ->
    _ = ets:new(sub_counters, [
        public, named_table, set, {write_concurrency, true}, {read_concurrency, true}
    ]),
    Duration = maps:get(duration, Opts),
    TotalDocuments = (maps:get(docid_end, Opts) - maps:get(docid_start, Opts) + 1),
    SubPerSec = TotalDocuments div Duration + 1,
    Ctx = new_sub_ctx(SubPerSec, TotalDocuments, maps:get(docid_start, Opts)),
    iterate_sub(Ctx, Opts),
    halt(0).

iterate_sub(#{done_count := DoneCount, total_count := TotalCount} = _Ctx, Opts) when
    DoneCount >= TotalCount
->
    io:format("Finished ~p of ~p~n", [DoneCount, TotalCount]),
    ok = print_sub_counters(Opts),
    ok;
iterate_sub(#{period_left_count := 0} = Ctx, Opts) ->
    CurrentTime = erlang:system_time(millisecond),
    PeriondElapsedTime = (CurrentTime - maps:get(period_start_time, Ctx)) div 1000,
    ok = print_sub_counters(Opts),
    sleep(1000 - PeriondElapsedTime),
    Ctx1 = Ctx#{
        period_start_time => erlang:system_time(millisecond),
        period_left_count => maps:get(period_count, Ctx)
    },
    iterate_sub(Ctx1, Opts);
iterate_sub(
    #{period_left_count := PeriodLeftCount, done_count := DoneCount, current_docid := CurrentDocId} =
        Ctx0,
    Opts
) ->
    Ctx1 = Ctx0#{
        period_left_count => PeriodLeftCount - 1,
        done_count => DoneCount + 1,
        current_docid => CurrentDocId + 1
    },
    ok = spawn_subscriber(Opts, CurrentDocId),
    iterate_sub(Ctx1, Opts).

new_sub_ctx(SubPerSec, TotalDocuments, CurrentDocId) ->
    #{
        period_start_time => erlang:system_time(millisecond),
        period_count => SubPerSec,
        period_left_count => SubPerSec,
        done_count => 0,
        total_count => TotalDocuments,
        current_docid => CurrentDocId
    }.

spawn_subscriber(Opts, DocId) ->
    _Pid = spawn_link(
        fun() -> run_subscriber(Opts, DocId) end
    ),
    ok.

run_subscriber(Opts, DocId) ->
    inc_sub_counter(started),
    Host = host(Opts),
    Port = maps:get(port, Opts),
    ClientId = clientid(Opts),
    TCPOpts = tcp_opts(Opts),
    {ok, Conn} = emqtt:start_link([
        {host, Host}, {port, Port}, {clientid, ClientId}, {tcp_opts, TCPOpts}
    ]),
    {ok, _Result} = emqtt:connect(Conn),
    PartIdStart = maps:get(partid_start, Opts),
    PartIdEnd = maps:get(partid_end, Opts),
    lists:foreach(
        fun(PartId) ->
            Topic = format_topic(Opts, DocId, PartId),
            {ok, _, _} = emqtt:subscribe(Conn, Topic, 1)
        end,
        lists:seq(PartIdStart, PartIdEnd)
    ),
    TimeToWait = maps:get(part_receive_timeout, Opts),
    Deadline = erlang:system_time(millisecond) + TimeToWait,
    wait_for_parts(Opts, Conn, PartIdEnd - PartIdStart + 1, Deadline).

wait_for_parts(Opts, Conn, NLeft, Deadline) ->
    Timeout = Deadline - erlang:system_time(millisecond),
    case Timeout > 0 of
        true ->
            wait_for_part(Opts, Conn, NLeft, Timeout, Deadline);
        false ->
            inc_sub_counter(receive_timeout),
            emqtt:disconnect(Conn)
    end.

wait_for_part(Opts, Conn, 0, _Timeout, _Deadline) ->
    inc_sub_counter(received_all),
    case maps:get(terminate_clients, Opts) of
        true -> emqtt:disconnect(Conn);
        false ->
            erlang:hibernate(?MODULE, wake_up, [])
    end;
wait_for_part(Opts, Conn, NLeft, Timeout, Deadline) ->
    receive
        {publish, #{retain := true}} ->
            wait_for_parts(Opts, Conn, NLeft - 1, Deadline)
    after Timeout ->
        inc_sub_counter(receive_timeout),
        emqtt:disconnect(Conn)
    end.

wake_up() ->
    io:format("waking up, shouldn't happen~n").

print_sub_counters(_Opts) ->
    io:format(
        "clients started: ~p, clients received all parts: ~p, clients receive timeout: ~p, processes: ~p~n", [
            get_sub_counter(started),
            get_sub_counter(received_all),
            get_sub_counter(receive_timeout),
            erlang:system_info(process_count)
        ]
    ).

inc_sub_counter(Name) ->
    ets:update_counter(sub_counters, Name, 1, {Name, 0}).

get_sub_counter(Name) ->
    case ets:lookup(sub_counters, Name) of
        [] ->
            0;
        [{Name, Value}] ->
            Value
    end.
