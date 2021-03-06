%%%-------------------------------------------------------------------
%%%
%%% riak_kv_qry_worker: Riak SQL per-query workers
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

%% @doc Under the queue manager accepting raw parsed and lexed queries
%%      from the user, workers take individual queries and communicate
%%      with eleveldb backend to execute the queries (with
%%      sub-queries), and hold the results until fetched back to the
%%      user.

-module(riak_kv_qry_worker).

-behaviour(gen_server).

%% OTP API
-export([start_link/1]).

%% gen_server callbacks
-export([
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3,

         %% decode_results/1 is now exported because it may be used in
         %% riak_kv_vnode to pre-decode query results at the vnode
         %% rather than here

         decode_results/1
        ]).

-include("riak_kv_ts.hrl").

-define(MAX_RUNNING_FSMS, 20).
-define(SUBQUERY_FSM_TIMEOUT, 10000).

%% accumulators for different types of query results.
-type rows_acc()      :: [{non_neg_integer(), list()}].
-type aggregate_acc() :: [{binary(), term()}].
-type group_by_acc()  :: {group_by, InitialState::term(), Groups::dict()}.

-record(state, {
          qry           = none                :: none | ?SQL_SELECT{},
          qid           = undefined           :: undefined | {node(), non_neg_integer()},
          sub_qrys      = []                  :: [integer()],
          receiver_pid                        :: pid(),
          %% overload protection:
          %% 1. Maintain a limited number of running fsms (i.e.,
          %%    process at most that many subqueries at a time):
          fsm_queue          = []                   :: [list()],
          n_running_fsms     = 0                    :: non_neg_integer(),
          max_running_fsms   = ?MAX_RUNNING_FSMS    :: pos_integer(),
          %% 2. Estimate query size (wget-style):
          n_subqueries_done  = 0                    :: non_neg_integer(),
          total_query_rows   = 0                    :: non_neg_integer(),
          total_query_data   = 0                    :: non_neg_integer(),
          max_query_data                            :: non_neg_integer(),
          %% For queries not backed by query buffers, results are
          %% accumulated in memory:
          result             = []                   :: rows_acc() | aggregate_acc() | group_by_acc(),
          %% Query buffer support
          qbuf_ref                 :: undefined | riak_kv_qry_buffers:qbuf_ref()
         }).

%%%===================================================================
%%% OTP API
%%%===================================================================
-spec start_link(RegisteredName::atom()) -> {ok, pid()} | ignore | {error, term()}.
start_link(RegisteredName) ->
    gen_server:start_link({local, RegisteredName}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

-spec init([RegisteredName::atom()]) -> {ok, #state{}}.
%% @private
init([]) ->
    pop_next_query(),
    {ok, new_state()}.

handle_call(_, _, State) ->
    {reply, {error, not_handled}, State}.

-spec handle_cast(term(), #state{}) -> {noreply, #state{}}.
%% @private
handle_cast(Msg, State) ->
    lager:info("Not handling cast message ~p", [Msg]),
    {noreply, State}.

%% @private
-spec handle_info(term(), #state{}) -> {noreply, #state{}}.
handle_info(pop_next_query, State1) ->
    QueryWork = riak_kv_qry_queue:blocking_pop(),
    {ok, State2} = execute_query(QueryWork, State1),
    {noreply, State2};

handle_info({{_, QId}, done}, #state{ qid = QId } = State) ->
    {noreply, maybe_subqueries_done(QId, State)};

handle_info({{SubQId, QId}, {results, Chunk}}, #state{qid = QId} = State1) ->
    State2 = add_subquery_result(SubQId, Chunk, State1),
    %% we may be able to return early from a query if we have enough rows to
    %% meet the OFFSET and LIMIT
    case is_result_limit_satisfied(State2) of
        true ->
            {noreply, subqueries_done(State2)};
        false ->
            {noreply, throttling_spawn_index_fsms(estimate_query_size(State2))}
    end;

handle_info({{SubQId, QId}, {error, Reason} = Error},
            #state{receiver_pid = ReceiverPid,
                   qid    = QId,
                   result = IndexedChunks}) ->
    lager:warning("Error ~p while collecting on QId ~p (~p);"
                  " dropping ~p chunks of data accumulated so far",
                  [Reason, QId, SubQId, IndexedChunks]),
    ReceiverPid ! Error,
    pop_next_query(),
    {noreply, new_state()};

handle_info({{_SubQId, QId1}, _}, State = #state{qid = QId2}) when QId1 =/= QId2 ->
    %% catches late results or errors such getting results for invalid QIds.
    lager:debug("Bad query id ~p (expected ~p)", [QId1, QId2]),
    {noreply, State};

handle_info(_Unexpected, State) ->
    lager:info("Not handling unexpected message \"~p\" in state ~p", [_Unexpected, State]),
    {noreply, State}.

-spec terminate(term(), #state{}) -> term().
%% @private
terminate(_Reason, _State) ->
    ok.

-spec code_change(term() | {down, term()}, #state{}, term()) -> {ok, #state{}}.
%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec new_state() -> #state{}.
new_state() ->
    MaxRunningFSMs = app_helper:get_env(riak_kv, timeseries_query_max_running_fsms,
                                        ?MAX_RUNNING_FSMS),
    #state{max_running_fsms = MaxRunningFSMs}.

prepare_fsm_options([], Acc) ->
    lists:reverse(Acc);
prepare_fsm_options([{{qry, ?SQL_SELECT{cover_context = CoverContext} = Q1}, {qid, QId}} | T], Acc) ->
    Table = Q1?SQL_SELECT.'FROM',
    Bucket = riak_kv_ts_util:table_to_bucket(Table),
    Timeout = {timeout, ?SUBQUERY_FSM_TIMEOUT},
    Me = self(),
    KeyConvFn = {riak_kv_ts_util, local_to_partition_key},
    Q2 = convert_query_to_cluster_version(Q1),
    CoverageParameter =
        case CoverContext of
            undefined ->
                {Q2, Bucket};
            _Defined ->
                %% if cover_context in the SQL record is *not* undefined, we've been
                %% given a mini coverage plan to map to a single vnode/quantum
                {ok, CoverProps} =
                    riak_kv_pb_coverage:checksum_binary_to_term(CoverContext),
                riak_client:vnode_target(CoverProps)
        end,
    Opts = [Bucket, none, Q2, Timeout, all, undefined, CoverageParameter, riak_kv_qry_coverage_plan, KeyConvFn],
    prepare_fsm_options(T, [[{raw, QId, Me}, Opts] | Acc]).


%% Convert the sql select record to a version that is safe to pass around the
%% cluster. Do not treat the result as a record in the local node, just as a
%% tuple.
convert_query_to_cluster_version(Query) ->
    Version = riak_core_capability:get({riak_kv, sql_select_version}),
    riak_kv_select:convert(Version, Query).

decode_results([]) ->
    [];
decode_results([{_,V}|Tail]) when is_binary(V) ->
    RObj = riak_object:from_binary(<<>>, <<>>, V),
    case riak_object:get_value(RObj) of
        <<>> ->
            %% record was deleted
            decode_results(Tail);
        Values ->
            %% FIXME should not have to convert to list
            [tuple_to_list(Values) | decode_results(Tail)]
    end.

%% Send a message to this process to get the next query.
pop_next_query() ->
    self() ! pop_next_query.

%%
execute_query({query, ReceiverPid, QId, [Qry1|_] = SubQueries, _DDL, QBufRef},
              State0) ->
    Indices = lists:seq(1, length(SubQueries)),
    ZQueries = lists:zip(Indices, SubQueries),
    %% all subqueries have the same select clause
    ?SQL_SELECT{'SELECT' = Sel} = Qry1,
    #riak_sel_clause_v1{initial_state = InitialState} = Sel,
    %% The initial state from the query compiler is used as a template for each
    %% new group row. Group by is a special case because the select is
    %% a typical aggregate, but we need a dict to store it for each group.
    InitialResult =
        case sql_select_calc_type(Qry1) of
            group_by ->
                {group_by, InitialState, dict:new()};
            _ ->
                InitialState
        end,
    SubQs = [{{qry, Q}, {qid, {I, QId}}} || {I, Q} <- ZQueries],
    FsmOptsList = prepare_fsm_options(SubQs, []),
    State = State0#state{qid            = QId,
                         receiver_pid   = ReceiverPid,
                         qry            = Qry1,
                         sub_qrys       = Indices,
                         fsm_queue      = FsmOptsList,
                         n_running_fsms = 0,
                         result         = InitialResult,
                         max_query_data = riak_kv_qry_buffers:get_max_query_data_size(),
                         qbuf_ref       = QBufRef},
    %% Start spawning index fsms, keeping at most
    %% #state.max_n_running_fsms running simultaneously.
    %% Notifications to proceed to the next fsm are sent to us, in the
    %% form of 'chunk_done' atom,
    {ok, throttling_spawn_index_fsms(State)}.

%%
throttling_spawn_index_fsms(#state{fsm_queue = [Opts | Rest],
                                   n_running_fsms = NRunning,
                                   max_running_fsms = SubqFsmSpawnThrottle} = State)
  when NRunning < SubqFsmSpawnThrottle ->
    {ok, _PID} = riak_kv_index_fsm_sup:start_index_fsm(node(), Opts),
    throttling_spawn_index_fsms(State#state{fsm_queue = Rest,
                                            n_running_fsms = NRunning + 1});
throttling_spawn_index_fsms(State) ->
    State.


estimate_query_size(#state{n_subqueries_done = NSubqueriesDone} = State)
  when NSubqueriesDone < 2 ->
    %% If not enough chunks are received, defer checks
    State;
estimate_query_size(#state{sub_qrys = []} = State) ->
    %% If all chunks are here (no more left in sub_qrys), consider it
    %% is safe to proceed.
    State;
estimate_query_size(#state{qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = aggregate}}} =
                        State) ->
    %% Aggregation alone does not increase the size of result (there
    %% is only one row returned)
    State;
estimate_query_size(#state{n_subqueries_done = NSubqueriesDone,
                           max_query_data    = MaxQueryData,
                           sub_qrys          = SubQrys,
                           result            = Result,
                           qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = group_by}} =
                               OrigQry} = State) ->
    %% Grouping queries will grow its result set to the number of
    %% unique values in the selection.  In the extreme case of
    %% grouping by a column of all-unique values, its size will be the
    %% size of the entire selection.
    CurrentTotalSize = erlang:external_size(Result),
    BytesPerChunk = CurrentTotalSize / NSubqueriesDone,
    ProjectedGrandTotal = round(CurrentTotalSize + (BytesPerChunk * length(SubQrys))),
    if ProjectedGrandTotal > MaxQueryData ->
            lager:info("Cancelling aggregating query because projected result size exceeds limit (~b > ~b, subqueries ~b of ~b done, query ~p)",
                       [ProjectedGrandTotal, MaxQueryData, NSubqueriesDone, length(SubQrys), OrigQry]),
            cancel_error_query(select_result_too_big, State);
       el/=se ->
            State
    end;
estimate_query_size(#state{total_query_data  = TotalQueryData,
                           total_query_rows  = TotalQueryRows,
                           n_subqueries_done = NSubqueriesDone,
                           max_query_data    = MaxQueryData,
                           qbuf_ref          = QBufRef,
                           sub_qrys          = SubQrys,
                           qry = ?SQL_SELECT{'LIMIT' = [Limit]} = OrigQry} = State)
  when QBufRef /= undefined ->

    %% query buffer-backed, has a LIMIT: consider the latter
    EstLimitData = round(Limit * (TotalQueryData / TotalQueryRows)),
    IsLimitTooBig = EstLimitData > MaxQueryData,

    %% but also check the grand total, for the case when LIMIT is big
    %% but WHERE range is still tiny
    EstTotalData = round(TotalQueryData + (TotalQueryData / NSubqueriesDone) * length(SubQrys)),
    IsWhereTooBig = EstTotalData > MaxQueryData,

    case IsLimitTooBig and IsWhereTooBig of
        true ->
            lager:info("Cancelling query with both projected LIMIT (~b) and total (~b) result size exceeding limit (~b), subqueries ~b of ~b done, query ~p)",
                       [EstLimitData, EstTotalData, MaxQueryData, NSubqueriesDone, length(SubQrys), OrigQry]),
            cancel_error_query(select_result_too_big, State);
        false ->
            State
    end;

estimate_query_size(#state{total_query_data  = TotalQueryData,
                           n_subqueries_done = NSubqueriesDone,
                           max_query_data    = MaxQueryData,
                           sub_qrys          = SubQrys,
                           qry               = OrigQry} = State) ->
    EstTotalData = round(TotalQueryData + (TotalQueryData / NSubqueriesDone) * length(SubQrys)),
    case EstTotalData > MaxQueryData of
        true ->
            lager:info("Cancelling query with projected total (~b) result size exceeding limit (~b), subqueries ~b of ~b done, query ~p)",
                       [EstTotalData, MaxQueryData, NSubqueriesDone, length(SubQrys), OrigQry]),
            cancel_error_query(select_result_too_big, State);
        false ->
            State
    end.


%%
add_subquery_result(SubQId, {selected, Chunk}, #state{sub_qrys = SubQs,
                                          total_query_data = TotalQueryData,
                                          total_query_rows = TotalQueryRows,
                                          n_subqueries_done = NSubqueriesDone,
                                          n_running_fsms = NRunning,
                                          qry = Query,
                                          result = Result} = State) ->
    case lists:member(SubQId, SubQs) of
        true ->
            try
                QueryResult = riak_kv_select:run_merge_on_chunk(SubQId, Chunk, Query, Result),
                NSubQ = lists:delete(SubQId, SubQs),
                ThisChunkData = erlang:external_size(Chunk),
                State#state{result            = QueryResult,
                            total_query_data  = TotalQueryData + ThisChunkData,
                            total_query_rows  = TotalQueryRows + chunk_length(Chunk),
                            n_subqueries_done = NSubqueriesDone + 1,
                            n_running_fsms    = NRunning - 1,
                            sub_qrys          = NSubQ}
            catch
                error:divide_by_zero ->
                    cancel_error_query(divide_by_zero, State);
                throw:{qbuf_internal_error, Reason} ->
                    cancel_error_query({qbuf_internal_error, Reason}, State)
            end;
        false ->
            lager:warning("unexpected chunk received for non-existing query ~p when state is ~p", [SubQId, State]),
            State
    end.

%% FIXME major hack, need to get rid of the estimation stuff
chunk_length(Chunk) when is_list(Chunk) -> length(Chunk);
chunk_length({group_by,_,Dict}) -> dict:size(Dict).

%%
-spec cancel_error_query(Error::any(), State1::#state{}) ->
        State2::#state{}.
cancel_error_query(Error, #state{qbuf_ref = QBufRef,
                                 receiver_pid = ReceiverPid}) ->
    catch riak_kv_qry_buffers:delete_qbuf(QBufRef),  %% is a noop on undefined
    ReceiverPid ! {error, Error},
    pop_next_query(),
    new_state().

%%
maybe_subqueries_done(QId, #state{qid = QId,
                                  sub_qrys = SubQQ} = State) ->
    case SubQQ of
        [] ->
            subqueries_done(State);
        _ ->
            % more sub queries are left to run
            State
    end.

subqueries_done(#state{receiver_pid = ReceiverPid} = State) ->
    QueryResult2 = prepare_final_results(State),
    %   send the results to the waiting client process
    ReceiverPid ! {ok, QueryResult2},
    pop_next_query(),
    % clean the state of query specfic data, ready for the next one
    new_state().

-spec prepare_final_results(#state{}) ->
                                   {[riak_pb_ts_codec:tscolumnname()],
                                    [riak_pb_ts_codec:tscolumntype()],
                                    [[riak_pb_ts_codec:ldbvalue()]]}.
prepare_final_results(#state{qbuf_ref = undefined,
                             result = IndexedChunks,
                             qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = rows} = Select} = Query}) ->
    %% sort by index, to reassemble according to coverage plan
    {_, R2} = lists:unzip(lists:sort(IndexedChunks)),
    prepare_final_results2(Select, maybe_apply_offset_limit(Query, lists:append(R2)));

prepare_final_results(#state{qbuf_ref = QBufRef,
                             qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = rows} = Select,
                                               'LIMIT'  = Limit,
                                               'OFFSET' = Offset}} = State) ->
    try riak_kv_qry_buffers:fetch_limit(QBufRef,
                                        riak_kv_qry_buffers:limit_to_scalar(Limit),
                                        riak_kv_qry_buffers:offset_to_scalar(Offset)) of
        {ok, {_ColNames, _ColTypes, FetchedRows}} ->
            prepare_final_results2(Select, FetchedRows);
        {error, qbuf_not_ready} ->
            riak_kv_qry_buffers:set_ready_waiting_process(
              QBufRef, fun(Msg) -> self() ! Msg end),
            receive
                {qbuf_ready, QBufRef} ->
                    prepare_final_results(State)
            after 60000 ->
                    lager:error("qbuf ~p took too long to signal ready to ship results", [QBufRef]),
                    cancel_error_query({qbuf_internal_error, "qbuf took too long to signal ready to ship results"}, State)
            end;
        {error, bad_qbuf_ref} ->
            %% the query buffer is gone: we can still retry (should we, really?)
            cancel_error_query(bad_qbuf_ref, State)
    catch
        Error:Reason ->
            lager:warning("Failed to fetch data from qbuf ~p: ~p:~p",
                          [QBufRef, Error, Reason]),
            cancel_error_query({qbuf_internal_error, "qbuf manager died/restarted mid-query"}, State)
    end;

prepare_final_results(#state{
        result = Aggregate1,
        qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = aggregate} = Select }} = State) ->
    try
        Aggregate2 = riak_kv_qry_compiler:finalise_aggregate(Select, Aggregate1),
        prepare_final_results2(Select, [Aggregate2])
    catch
        error:divide_by_zero ->
            cancel_error_query(divide_by_zero, State)
    end;
prepare_final_results(#state{
        result = {group_by, _, Dict},
        qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = group_by} = Select }} = State) ->
    try
        FinaliseFn =
            fun(_,V,Acc) ->
                [riak_kv_qry_compiler:finalise_aggregate(Select, V) | Acc]
            end,
        GroupedRows = dict:fold(FinaliseFn, [], Dict),
        prepare_final_results2(Select, GroupedRows)
    catch
        error:divide_by_zero ->
            cancel_error_query(divide_by_zero, State)
    end.

%%
prepare_final_results2(#riak_sel_clause_v1{col_return_types = ColTypes,
                                           col_names = ColNames}, Rows) ->
    {ColNames, ColTypes, Rows}.

%% Return the `calc_type' from a query.
sql_select_calc_type(?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = Type}}) ->
    Type.

maybe_apply_offset_limit(?SQL_SELECT{'OFFSET' = [], 'LIMIT' = []}, Rows) ->
    Rows;
maybe_apply_offset_limit(?SQL_SELECT{'OFFSET' = [Offset], 'LIMIT' = []}, Rows) when is_integer(Offset), Offset >= 0 ->
    safe_nthtail(Offset, Rows);
maybe_apply_offset_limit(?SQL_SELECT{'OFFSET' = [], 'LIMIT' = [Limit]}, Rows) when is_integer(Limit), Limit >= 0 ->
    lists:sublist(Rows, Limit);
maybe_apply_offset_limit(?SQL_SELECT{'OFFSET' = [Offset], 'LIMIT' = [Limit]}, Rows) when is_integer(Offset), is_integer(Limit), Offset >= 0, Limit >= 0 ->
    lists:sublist(safe_nthtail(Offset, Rows), Limit);
maybe_apply_offset_limit(_, _) ->
    [].

%% safe because this function will not throw an exception if the list is not
%% longer than the offset, unlike lists:nthtail/2
safe_nthtail(0, Rows) ->
    Rows;
safe_nthtail(_, []) ->
    [];
safe_nthtail(Offset, [_|Tail]) ->
    safe_nthtail(Offset-1, Tail).

%% returns true if the number of rows that have been received for the
%% current query is greater than or equal to the OFFSET and LIMIT. If
%% ORDER BY or GROUP BY has been specified then we need all sub query
%% results and cannot return early.
is_result_limit_satisfied(#state{total_query_rows = TotalRows, qry = Query}) ->
    case find_query_limit(Query?SQL_SELECT.'OFFSET', Query?SQL_SELECT.'LIMIT',
                          Query?SQL_SELECT.group_by, Query?SQL_SELECT.'ORDER BY') of
        undefined ->
            false;
        RequiredRows ->
            TotalRows >= RequiredRows
    end.

find_query_limit(Offset, Limit, GroupBy, OrderBy) when GroupBy /= [] orelse
                                                       OrderBy /= [] orelse
                                                       (Offset == [] andalso Limit == []) ->
    undefined;
find_query_limit(Offset, Limit, _, _) ->
    limit_number(Offset) + limit_number(Limit).

limit_number([ ]) -> 0;
limit_number([V]) -> V.

%%%===================================================================
%%% Unit tests
%%%===================================================================

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

prepare_final_results_test() ->
    Rows = [[12, <<"windy">>], [13, <<"windy">>]],
    ?assertEqual(
        {[<<"a">>, <<"b">>], [sint64, varchar], Rows},
        prepare_final_results(
            #state{
                qry =
                    ?SQL_SELECT{
                        'SELECT' = #riak_sel_clause_v1{
                            col_names = [<<"a">>, <<"b">>],
                            col_return_types = [sint64, varchar],
                            calc_type = rows
                         }
                    },
                result = [{1, Rows}]})
    ).

estimate_query_size_limit_applies_to_aggregating_queries_test() ->
    BigData = [<<"BIGFATDATA">>],
    check_states(
      #state{qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = group_by}},
             result            = BigData,
             max_query_data    = erlang:external_size(BigData) - 1,
             sub_qrys          = lists:seq(1, 100)}).

estimate_query_size_limit_applies_to_regular_queries_test() ->
    check_states(
      #state{qry = ?SQL_SELECT{'SELECT' = #riak_sel_clause_v1{calc_type = rows}},
             total_query_data  = 10,
             max_query_data    = 30,
             sub_qrys          = lists:seq(1, 100)}).

check_states(State) ->
    lists:foreach(
      fun({StateN, Outcome}) -> ok = check_states2(StateN, Outcome) end,
      [{State#state{n_subqueries_done = 1}, passing},
       {State#state{n_subqueries_done = 2}, cancelled},
       {State#state{n_subqueries_done = 3}, cancelled}]).

check_states2(State, passing) ->
    ?assertEqual(estimate_query_size(State), State);
check_states2(State, cancelled) ->
    ?assertEqual(estimate_query_size(State#state{receiver_pid = self()}), new_state()),
    receive
        {error, select_result_too_big} ->
            ok
    after 1000 ->
            didnt_receive_select_result_too_big_error
    end.

-endif.
