-module(pico_riak_vnode).

-behavior(riak_core_vnode).

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_exit/3,
         ready_to_exit/0,
         set_vnode_forwarding/2,
         handle_overload_command/3,
         handle_overload_info/2]).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(state,
        { index           %% Partition index
        , vnode_id        %% VNode identifier
        , ref
        })

%% ===================================================================
%% VNode Behavior Callbacks
%% ===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Idx]) ->
    %% Initiate our bitcask backend here
    %%
    %% Default to the current directory and name the top-level "data"
    %% with a sub-directory named bitcask, ensure the directories are
    %% created.
    {ok, BCConfig} = application:get_env(pico_riak, bitcask_config),

    BCHandle = pico_riak_bitcask_backend:start(Partition, BCConfig),

    State = #state{ index = Idx, ref = BCHandle },
    
    Workers = application:get_env(pico_riak, vnode_workers, 1),
    FoldWorkerPool = {pool, pico_riak_vnode_worker, Workers, [Pos]},

    {ok, State, [FoldWorkerPool]}.

%% ===================================================================
%% VNode Command Callbacks
%% ===================================================================
handle_command({get, Key}, _Sender, State) ->
    R = do_get(Key, State),
    reply(R, State).

handle_command({}, _Sender, State) ->
    .

-spec do_get(key(), #state{}) -> {ok, dvvset:clock()} | {error, term()}.
do_get(Key, State) ->
    case catch pico_riak_backend:get(State#state.bitcask_handle, Key) of
        {ok, DVV}       -> {ok, DVV};
        {error, Error}  -> {error, Error};
        {'EXIT', Error} -> {error, Error}
    end.

-spec reply(term(), #state{}) -> {reply, {vnode, integer(), atom(), term()}, #state{}}.
reply(Reply, #state{index=Index}=State) ->
    {reply, {vnode, Index, node(), Reply}, State}.
