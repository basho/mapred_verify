-module(mapred_verifiers).

-compile([export_all]).

%% @type type() = bucket|entries

%% @spec map(type(), [term()], integer()) -> boolean()
%% @doc Checks that the Result is TotalEntries in length.
%%      Returns 'true' if so, false if not.
map(missing, [_], 0) ->
    %% the bare "JS map" case is hard to test because Javascript map
    %% phases include an error result if the bucket/key is missing,
    %% unlike Erlang map phases
    %% the "Erlang map" case has been setup to behave similarly for
    %% the purpose of excersizing the "include_notfound" argument
    io:format("Warning: 1 Map Result?"),
    true;
map(missing, Result, TotalEntries) ->
    io:format("Got ~p notfounds, Expecting: ~p...", [length(Result), TotalEntries]),
    NotFoundMatch = fun(X) ->
                            case X of
                                {error, _} -> % matches erlang notfound pattern
                                    true;
                                {not_found, _, _} -> % matches javascript notfound pattern
                                    true;
                                _ ->
                                    false
                            end
                    end,
    length(Result) == TotalEntries andalso lists:all(NotFoundMatch, Result);
map(_Type, Result, TotalEntries) ->
    io:format("Got ~p, Expecting: ~p...", [length(Result), TotalEntries]),
    length(Result) == TotalEntries.

%% @spec simple(type(), [term()], integer()) -> boolean()
%% @doc Checks that Result is a single-element list, that element
%%      being an integer less than or equal to TotalEntries.
simple(_Type, Result, TotalEntries) ->
    [R] = Result,
    is_integer(R) andalso R =< TotalEntries.

%% @spec sorted(type(), [term()], integer()) -> boolean()
%% @doc Checks that Result is a list whose length is less than or
%%      equal to TotalEntries, and whose elements are sorted.
sorted(_Type, Result, TotalEntries) ->
    length(Result) =< TotalEntries andalso
        Result == lists:sort(Result).
