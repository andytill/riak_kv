%%%-------------------------------------------------------------------
%%%
%%% riak_kv_select: Upgrade and downgrade for the riak_select_v* records.
%%%
%%% Copyright (C) 2016 Basho Technologies, Inc. All rights reserved
%%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%%
%%%-------------------------------------------------------------------

-module(riak_kv_select).

-export([convert/2]).
-export([current_version/0]).
-export([first_version/0]).
-export([is_sql_select_record/1]).
-export([run_merge_on_chunk/4]).
-export([run_select_on_chunk/3]).

-include("riak_kv_ts.hrl").

-type select_rec_version() :: v3 | v2 | v1.
-export_type([select_rec_version/0]).

%% Return the version as an integer that the select record is currently using.
-spec current_version() -> select_rec_version().
current_version() ->
    select_record_version(?SQL_SELECT_RECORD_NAME).

%% Return the first version that was declared for the select record.
-spec first_version() -> select_rec_version().
first_version() ->
    v1.

%% Convert a select record to a different version.
-spec convert(select_rec_version(), Select1::tuple()) -> Select2::tuple().
convert(Version, Select) when is_atom(Version) ->
    CurrentVersion = select_record_version(element(1, Select)),
    case compare(Version, CurrentVersion) of
        equal ->
            Select;
        greater_than  ->
            VersionSteps = sublist_elements(CurrentVersion, Version, [v1,v2,v3]),
            upgrade_select(VersionSteps, Select);
        less_than ->
            VersionSteps = sublist_elements(CurrentVersion, Version, [v3,v2,v1]),
            downgrade_select(VersionSteps, Select)
    end.

%% Return a sublist within a list when only the start and end elements within
%% the list are known, not the index.
sublist_elements(_, _, []) ->
    [];
sublist_elements(From, To, [From|Tail]) ->
    [From|sublist_elements_inner(To, Tail)];
sublist_elements(From, To, [_|Tail]) ->
    sublist_elements(From, To, Tail).

%%
sublist_elements_inner(_, []) ->
    [];
sublist_elements_inner(To, [To|_]) ->
    [To];
sublist_elements_inner(To, [Other|    Tail]) ->
    [Other|sublist_elements_inner(To, Tail)].

%%
compare(V, V)  -> equal;
compare(V1, V2) when V1 > V2 -> greater_than;
compare(_, _) -> less_than.

%% Iterate over the versions and upgrade the record, from 1 to 2, 2 to 3 etc.
upgrade_select([_], Select) ->
    Select;
upgrade_select([v1,v2 = To|Tail],
               #riak_select_v1{'SELECT'      = Select,
                               'FROM'        = From,
                               'WHERE'       = Where,
                               'ORDER BY'    = OrderBy,
                               'LIMIT'       = Limit,
                               helper_mod    = HelperMod,
                               is_executable = IsExecutable,
                               type          = Type,
                               cover_context = CoverContext,
                               partition_key = PartitionKey,
                               local_key     = LocalKey}) ->
    Select2 = #riak_select_v2{
                 'SELECT'      = Select,
                 'FROM'        = From,
                 'WHERE'       = Where,
                 'ORDER BY'    = OrderBy,
                 'LIMIT'       = Limit,
                 helper_mod    = HelperMod,
                 is_executable = IsExecutable,
                 type          = Type,
                 cover_context = CoverContext,
                 partition_key = PartitionKey,
                 local_key     = LocalKey,
                 group_by      = ?GROUP_BY_DEFAULT
                },
    upgrade_select([To|Tail], Select2);
upgrade_select([v2,v3 = To|Tail],
               #riak_select_v2{'SELECT'      = Select,
                               'FROM'        = From,
                               'WHERE'       = Where,
                               'ORDER BY'    = OrderBy,
                               'LIMIT'       = Limit,
                               helper_mod    = HelperMod,
                               is_executable = IsExecutable,
                               type          = Type,
                               cover_context = CoverContext,
                               partition_key = PartitionKey,
                               local_key     = LocalKey,
                               group_by      = GroupBy}) ->
    Select2 = #riak_select_v3{
                 'SELECT'      = Select,
                 'FROM'        = From,
                 'WHERE'       = Where,
                 'ORDER BY'    = OrderBy,
                 'LIMIT'       = Limit,
                 'OFFSET'      = [],
                 helper_mod    = HelperMod,
                 is_executable = IsExecutable,
                 type          = Type,
                 cover_context = CoverContext,
                 partition_key = PartitionKey,
                 local_key     = LocalKey,
                 group_by      = GroupBy
                },
    upgrade_select([To|Tail], Select2).


%% Iterate over the versions backwards to downgrade, from 3 to 2 then 2 to 1 etc.
downgrade_select([_], Select) ->
    Select;
downgrade_select([v2,v1=To|Tail],
               #riak_select_v2{'SELECT'      = Select,
                               'FROM'        = From,
                               'WHERE'       = Where,
                               'ORDER BY'    = OrderBy,
                               'LIMIT'       = Limit,
                               helper_mod    = HelperMod,
                               is_executable = IsExecutable,
                               type          = Type,
                               cover_context = CoverContext,
                               partition_key = PartitionKey,
                               local_key     = LocalKey,
                               group_by      = _GroupBy}) ->
    Select2 = #riak_select_v1{
                 'SELECT'      = Select,
                 'FROM'        = From,
                 'WHERE'       = Where,
                 'ORDER BY'    = OrderBy,
                 'LIMIT'       = Limit,
                 helper_mod    = HelperMod,
                 is_executable = IsExecutable,
                 type          = Type,
                 cover_context = CoverContext,
                 partition_key = PartitionKey,
                 local_key     = LocalKey
    },
    downgrade_select([To|Tail], Select2);
downgrade_select([v3,v2=To|Tail],
               #riak_select_v3{'SELECT'      = Select,
                               'FROM'        = From,
                               'WHERE'       = Where,
                               'ORDER BY'    = OrderBy,
                               'LIMIT'       = Limit,
                               'OFFSET'      = _Offset,
                               helper_mod    = HelperMod,
                               is_executable = IsExecutable,
                               type          = Type,
                               cover_context = CoverContext,
                               partition_key = PartitionKey,
                               local_key     = LocalKey,
                               group_by      = GroupBy}) ->
    Select2 = #riak_select_v2{
                 'SELECT'      = Select,
                 'FROM'        = From,
                 'WHERE'       = Where,
                 'ORDER BY'    = OrderBy,
                 'LIMIT'       = Limit,
                 helper_mod    = HelperMod,
                 is_executable = IsExecutable,
                 type          = Type,
                 cover_context = CoverContext,
                 partition_key = PartitionKey,
                 local_key     = LocalKey,
                 group_by      = GroupBy
                },
    downgrade_select([To|Tail], Select2).

%%
select_record_version(RecordName) ->
    case RecordName of
        riak_select_v1 -> v1;
        riak_select_v2 -> v2;
        riak_select_v3 -> v3
    end.

%%
-spec is_sql_select_record(tuple()) -> boolean().
is_sql_select_record(#riak_select_v1{ }) -> true;
is_sql_select_record(#riak_select_v2{ }) -> true;
is_sql_select_record(#riak_select_v3{ }) -> true;
is_sql_select_record(_)                  -> false.

%%
run_select_on_chunk(Chunk, ?SQL_SELECT{} =  Query, Acc) ->
    case sql_select_calc_type(Query) of
        rows ->
            run_select_on_rows_chunk(sql_select_clause(Query), Chunk, undefined);
        aggregate ->
            run_select_on_aggregate_chunk(sql_select_clause(Query), Chunk, Acc);
        group_by ->
            run_select_on_group(Query, sql_select_clause(Query), Chunk, Acc)
    end.

%% ------------------------------------------------------------
%% Helper function to return decoded query results for the current
%% Chunk:
%%
%%   if already decoded, simply returns the decoded data
%%
%%   if not, decodes and returns
%% ------------------------------------------------------------

% rows_in_chunk({_, Chunk}) ->
%     length(Chunk);
% rows_in_chunk(Chunk) ->
%     length(Chunk).


%%
run_select_on_group(Query, SelClause, Chunk, QueryResult1) ->
    lists:foldl(
        fun(Row, Acc) ->
            run_select_on_group_row(Query, SelClause, Row, Acc)
        end, QueryResult1, Chunk).

%%
run_select_on_group_row(Query, SelClause, Row, QueryResult1) ->
    {group_by, InitialGroupState, Dict1} = QueryResult1,
    Key = select_group(Query, Row),
    Aggregate1 =
        case dict:find(Key, Dict1) of
            error ->
                prepare_group_by_initial_state(Row, InitialGroupState);
            {ok, AggregateX} ->
                AggregateX
        end,
    Aggregate2 = riak_kv_qry_compiler:run_select(SelClause, Row, Aggregate1),
    Dict2 = dict:store(Key, Aggregate2, Dict1),
    {group_by, InitialGroupState, Dict2}.

prepare_group_by_initial_state(Row, InitialState) ->
    [prepare_group_by_initial_state2(Row, Col) || Col <- InitialState].

prepare_group_by_initial_state2(Row, InitFn) when is_function(InitFn) ->
    InitFn(Row);
prepare_group_by_initial_state2(_, InitVal) ->
    InitVal.

%%
select_group(Query, Row) ->
    GroupByFields = sql_select_group_by(Query),
    select_group2(GroupByFields, Row).

select_group2([], _) ->
    [];
select_group2([{N,_}|Tail], Row) when is_integer(N) ->
    [lists:nth(N, Row)|select_group2(Tail, Row)];
select_group2([{GroupByTimeFn,_}|Tail], Row) when is_function(GroupByTimeFn) ->
    [GroupByTimeFn(Row)|select_group2(Tail, Row)].

%% Run the selection clause on results that accumulate rows
run_select_on_rows_chunk(SelClause, DecodedChunk, undefined) ->
    [riak_kv_qry_compiler:run_select(SelClause, Row) || Row <- DecodedChunk];
run_select_on_rows_chunk(SelClause, DecodedChunk, QBufRef) ->
    IndexedChunks =
        [riak_kv_qry_compiler:run_select(SelClause, Row) || Row <- DecodedChunk],
    try riak_kv_qry_buffers:batch_put(QBufRef, IndexedChunks) of
        ok ->
            ok;
        {error, Reason} ->
            throw({qbuf_internal_error, Reason})
    catch
        Error:Reason ->
            lager:warning("Failed to send data to qbuf ~p serving subquery ~p of ~p: ~p:~p",
                          [QBufRef, x, SelClause, Error, Reason]),
            throw({qbuf_internal_error, "qbuf manager died/restarted mid-query"})
    end.

%%
run_select_on_aggregate_chunk(SelClause, DecodedChunk, QueryResult1) ->
    lists:foldl(
        fun(E, Acc) ->
            riak_kv_qry_compiler:run_select(SelClause, E, Acc)
        end, QueryResult1, DecodedChunk).

%%
run_merge_on_chunk(SubQId, Chunk, Query, Acc) ->
    case sql_select_calc_type(Query) of
        rows ->
            [{SubQId,Chunk}|Acc];
        aggregate ->
            run_merge_on_aggregate(sql_select_merge_fns(Query), Chunk, Acc);
        group_by ->
            run_merge_on_group_by(sql_select_merge_fns(Query), Chunk, Acc)
    end.

run_merge_on_aggregate([], [], []) ->
    [];
run_merge_on_aggregate([MergeFn|MergeTail], [Col|ColTail], [AccCol|Acc]) ->
    [MergeFn(Col,AccCol)|run_merge_on_aggregate(MergeTail,ColTail,Acc)].

run_merge_on_group_by(AggMergeFns, {group_by, _, ChunkDict}, {group_by, InitialState, AccDict1}) ->
    AccDict2 = dict:merge(
        fun(_K, V1, V2) ->
            run_merge_on_aggregate(AggMergeFns, V1, V2)
        end,
        ChunkDict, AccDict1),
    {group_by, InitialState, AccDict2}.

sql_select_calc_type(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = Type}}) ->
    Type.

%% Return the selection clause from a query
sql_select_clause(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{compiled_clause = Clause}}) ->
    Clause.

sql_select_group_by(?SQL_SELECT{group_by = GroupBy}) ->
    GroupBy.

sql_select_merge_fns(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{result_merger_fns = MergeFns}}) ->
    MergeFns.

%%%
%%% TESTS
%%%

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

new_select_v1() ->
    {riak_select_v1, undefined, << >>, [], [], [], undefined, none, false,
     sql, undefined, undefined}.

new_select_v2() ->
    {riak_select_v2, undefined, << >>, [], [], [], undefined, none, false,
     sql, undefined, undefined, ?GROUP_BY_DEFAULT}.

upgrade_select_1_2_test() ->
    ?assertEqual(
        new_select_v2(),
        convert(v2, new_select_v1())
    ).

downgrade_select_2_1_test() ->
    ?assertEqual(
        new_select_v1(),
        convert(v1, new_select_v2())
    ).

convert_when_equal_1_1_test() ->
    Select = setelement(2,new_select_v1(),oko),
    ?assertEqual(
        Select,
        convert(v1, Select)
    ).

convert_when_equal_2_2_test() ->
    Select = setelement(2,new_select_v2(),oko),
    ?assertEqual(
        Select,
        convert(v2, Select)
    ).

sublist_elements_1_test() ->
    ?assertEqual(
        [b,c,d],
        sublist_elements(b,d,[a,b,c,d,e])
    ).

sublist_elements_2_test() ->
    ?assertEqual(
        [a,b],
        sublist_elements(a,b,[a,b,c,d,e])
    ).

-endif.
