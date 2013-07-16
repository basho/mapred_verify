-module(mapred_verify).

-export([main/1]).
-export([do_verification/1]).

-define(BUCKET, <<"mr_validate">>).
-define(PROP(K, L), proplists:get_value(K, L)).

main([]) ->
    usage();
main(Args) ->
    case setup_environment(Args) of
        {ok, Props} ->
            case do_verification(Props) of
                Fails when is_integer(Fails) ->
                    %% exit 0 on success, non-zero on failure
                    erlang:halt(Fails);
                _ ->
                    %% just shutdown normally when not running tests
                    ok
            end;
        error ->
            usage()
    end.

do_verification(Props) ->
    {T1, T2, T3} = erlang:now(),
    random:seed(T1, T2, T3),
    {ok, Client} = riak:client_connect(?PROP(node, Props)),
    KeyCount = ?PROP(keycount, Props),
    case ?PROP(populate, Props) of
        true ->
            io:format("Clearing old data from ~p~n", [?BUCKET]),
            ok = clear_bucket(Client, ?BUCKET, erlang:round(KeyCount * 1.25)),
            io:format("Populating new data to ~p~n", [?BUCKET]),
            ok = populate_bucket(Client, ?BUCKET,
                                 ?PROP(bodysize, Props), KeyCount);
        false ->
            ok
    end,
    case ?PROP(runjobs, Props) of
        true ->
            io:format("Verifying map/reduce jobs~n"),
            run_jobs(Client, ?BUCKET, KeyCount, ?PROP(testdef, Props));
        false ->
            ok
    end.

clear_bucket(_Client, _BucketName, 0) ->
    ok;
clear_bucket(Client, BucketName, EntryNum) ->
    Key = entry_num_to_key(EntryNum),
    case Client:delete(BucketName, Key, 1) of
        R when R =:= ok orelse R =:= {error, notfound} ->
            clear_bucket(Client, BucketName, EntryNum - 1);
        Error ->
            io:format("Error: ~p~n", [Error])
    end.

populate_bucket(_Client, _BucketName, _KeySize, 0) ->
    ok;
populate_bucket(Client, BucketName, KeySize, EntryNum) ->
    Key = entry_num_to_key(EntryNum),
    Obj = riak_object:new(BucketName, Key, generate_body(KeySize)),
    LinkObj = add_links(Obj, BucketName, EntryNum),
    ok = Client:put(LinkObj, 0),
    populate_bucket(Client, BucketName, KeySize, EntryNum - 1).

add_links(Obj, BucketName, EntryNum) ->
    MD0 = riak_object:get_metadata(Obj),
    MD1 = dict:store(<<"Links">>,
                     [{{BucketName, entry_num_to_key(EntryNum-1)},<<"prev">>},
                      {{BucketName, entry_num_to_key(EntryNum+1)},<<"next">>}],
                     MD0),
    riak_object:update_metadata(Obj, MD1).

run_jobs(Client, Bucket, KeyCount, TestFile) ->
    Tests = test_descriptions(TestFile),
    F = fun({Label, {Job, Verifier}}, Acc) ->
                Acc + verify_job(Client, Bucket, KeyCount, Label, Job, Verifier)
        end,
    lists:foldl(F, 0, Tests).

test_descriptions(TestFile) ->
    case file:consult(TestFile) of
        {ok, Tests} -> Tests;
        Error ->
            io:format("Error loading test definition file: ~p~n", [Error]),
            exit(-1)
    end.

verify_job(Client, Bucket, KeyCount, Label, JobDesc, Verifier) ->
    case re:run(Label, "link", [caseless]) of
        {match, _} ->
            %% The link phase of a MapReduce query does not return any
            %% results on a missing key, so it makes checking for the
            %% presence of notfound in the map results irrelevant.
            Tests = [{"discrete entries", fun verify_entries_job/5},
                     {"full bucket mapred", fun verify_bucket_job/5},
                     {"filtered bucket mapred", fun verify_filter_job/5}];
        _ ->
            Tests = [{"discrete entries", fun verify_entries_job/5},
                     {"full bucket mapred", fun verify_bucket_job/5},
                     {"filtered bucket mapred", fun verify_filter_job/5},
                     {"missing object", fun verify_missing_job/5},
                     {"missing object twice", fun verify_missing_twice_job/5}]
                end,
    io:format("Running ~p~n", [Label]),
    lists:foldl(
      fun({Type, Fun}, Acc) ->
              io:format("   Testing ~s...", [Type]),
              case Fun(Client, Bucket, KeyCount, JobDesc, Verifier) of
                  {true, ETime} ->
                      io:format("OK (~p)~n", [ETime]),
                      Acc;
                  {false, _} ->
                      io:format("FAIL~n"),
                      Acc + 1
              end
      end, 0, Tests).

verify_missing_job(Client, _Bucket, _KeyCount, JobDesc, Verifier) ->
    Inputs = [{<<"mrv_missing">>, <<"mrv_missing">>}],
    Start = erlang:now(),
    {_, [Node, _]} = Client,
    {ok, Result} = rpc:call(Node, riak_kv_mrc_pipe, mapred, [Inputs, JobDesc]),
    End = erlang:now(),
    {mapred_verifiers:Verifier(missing, Result, 1),
     erlang:round(timer:now_diff(End, Start) / 1000)}.

verify_missing_twice_job(Client, _Bucket, _KeyCount, JobDesc, Verifier) ->
    Inputs = [{<<"mrv_missing">>, <<"mrv_missing">>},
              {<<"mrv_missing">>, <<"mrv_missing">>}],
    Start = erlang:now(),
    {_, [Node, _]} = Client,
    {ok, Result} = rpc:call(Node, riak_kv_mrc_pipe, mapred, [Inputs, JobDesc]),
    End = erlang:now(),
    {mapred_verifiers:Verifier(missing, Result, 2),
     erlang:round(timer:now_diff(End, Start) / 1000)}.

verify_filter_job(Client, Bucket, KeyCount, JobDesc, Verifier) ->
    Filter = [[<<"or">>,
               [[<<"ends_with">>,<<"1">>]],
               [[<<"ends_with">>,<<"5">>]]
              ]],
    Start = erlang:now(),
    {_, [Node, _]} = Client,
    {ok, Result} = rpc:call(Node, riak_kv_mrc_pipe, mapred,
                            [{Bucket,Filter}, JobDesc]),
    End = erlang:now(),
    Inputs = compute_filter(KeyCount, Filter),
    {mapred_verifiers:Verifier(filter, Result, length(Inputs)),
     erlang:round(timer:now_diff(End, Start) / 1000)}.

verify_bucket_job(Client, Bucket, KeyCount, JobDesc, Verifier) ->
    Start = erlang:now(),
    {_, [Node, _]} = Client,
    {ok, Result} = rpc:call(Node, riak_kv_mrc_pipe, mapred,
                            [Bucket, JobDesc, 600000]),
    End = erlang:now(),
    {mapred_verifiers:Verifier(bucket, Result, KeyCount),
     erlang:round(timer:now_diff(End, Start) / 1000)}.

verify_entries_job(Client, Bucket, KeyCount, JobDesc, Verifier) ->
    Inputs = select_inputs(Bucket, KeyCount),
    Start = erlang:now(),
    {_, [Node, _]} = Client,
    {ok, Result} = rpc:call(Node, riak_kv_mrc_pipe, mapred, [Inputs, JobDesc]),
    End = erlang:now(),
    {mapred_verifiers:Verifier(entries, Result, length(Inputs)),
     erlang:round(timer:now_diff(End, Start) / 1000)}.

entry_num_to_key(EntryNum) ->
    list_to_binary(["mrv", integer_to_list(EntryNum)]).

select_inputs(Bucket, KeyCount) ->
    [{Bucket, entry_num_to_key(EntryNum)}
     || EntryNum <- lists:seq(1, KeyCount),
        random:uniform(2) == 2].

compute_filter(KeyCount, FilterSpec) ->
    {ok, Filter} = riak_kv_mapred_filters:build_exprs(FilterSpec),
    lists:filter(fun(K) -> run_filters(Filter, K) end,
                 [ entry_num_to_key(EntryNum)
                   || EntryNum <- lists:seq(1, KeyCount) ]).

run_filters([{Mod,Fun,Args}|Rest], Acc) ->
    Filter = Mod:Fun(Args),
    run_filters(Rest, Filter(Acc));
run_filters([], Acc) ->
    Acc.

usage() ->
    io:format("~p"
              " -s <Erlang node name>"
              " -c <path to top of Riak source tree>"
              " [-k keycount]"
              " [-b objectsize{k|b}]"
              " [-p true|false]"
              " [-j true|false]"
              " [-f <filename>]~n",
              [?MODULE]).

setup_environment(Args) ->
    Extracted = [ {Name, extract_arg(Flag, Type, Args, Default)}
                  || {Name, Flag, Type, Default} <-
                         [{node, "-s", atom, error},
                          {path, "-c", string, error},
                          {keycount, "-k", integer, 1000},
                          {bodysize, "-b", kbinteger, 1},
                          {populate, "-p", atom, false},
                          {runjobs, "-j", atom, false},
                          {testdef, "-f", string, "priv/tests.def"}]],
    case {?PROP(node, Extracted), ?PROP(path, Extracted)} of
        {N, P} when N =/= error, P =/= error ->
            case setup_code_paths(P) of
                ok ->
                    case setup_networking() of
                        ok ->
                            {ok, Extracted};
                        error ->
                            error
                    end;
                error ->
                    error
            end;
        _ ->
            error
    end.

setup_code_paths(Path) ->
    CorePath = filename:join([Path, "deps", "riak_core", "ebin"]),
    KVPath = filename:join([Path, "deps", "riak_kv", "ebin"]),
    LukePath = filename:join([Path, "deps", "luke", "ebin"]),
    setup_paths([{riak_core, CorePath}, {riak_kv, KVPath},
                 {luke, LukePath}]).

setup_paths([]) ->
    ok;
setup_paths([{Label, Path}|T]) ->
    case filelib:is_dir(Path) of
        true ->
            code:add_pathz(Path),
            setup_paths(T);
        false ->
            io:format("ERROR: Path for ~p (~p) not found"
                      " or doesn't point to a directory.~n",
                      [Label, Path]),
            error
    end.

extract_arg(Name, Type, [Name,Val|_Rest], _Default) ->
    case Type of
        string    -> Val;
        integer   -> list_to_integer(Val);
        kbinteger -> parse_kbinteger(Val);
        atom      -> list_to_atom(Val)
    end;
extract_arg(Name, Type, [_,_|Rest], Default) ->
    extract_arg(Name, Type, Rest, Default);
extract_arg(_Name, _Type, _, Default) ->
    Default.

setup_networking() ->
    {T1, T2, T3} = erlang:now(),
    random:seed(T1, T2, T3),
    Name = "mapred_verify" ++ integer_to_list(random:uniform(100)),
    {ok, H} = inet:gethostname(),
    {ok, {O1, O2, O3, O4}} = inet:getaddr(H, inet),
    NodeName = list_to_atom(lists:flatten(
                              io_lib:format("~s@~p.~p.~p.~p",
                                            [Name, O1, O2, O3, O4]))),
    {ok, _} = net_kernel:start([NodeName, longnames]),
    erlang:set_cookie(NodeName, riak),
    ok.

%% parse things like
%%   "12"   == 12
%%   "100b" == 100
%%   "1k"   == 1024
%%   "40k"  == 40960
parse_kbinteger(KeySpec) ->
    [UnitChar|RevVal] = lists:reverse(KeySpec),
    Unit = case UnitChar of
               $k ->
                   1024;
               $b ->
                   1;
               C when 0 =< C, C =< 9 ->
                   1
           end,
    Size = lists:reverse(RevVal),
    list_to_integer(Size) * Unit.

generate_body(Size) ->
    list_to_binary([lists:duplicate(Size-1, $0),"1"]).
