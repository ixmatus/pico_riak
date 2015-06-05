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
        , backend         %% Bitcask backend state
        })

%% ===================================================================
%% VNode Behavior Callbacks
%% ===================================================================

start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Idx]) ->

    {ok, BCConfig} = application:get_env(pico_riak, bitcask_config),

    %% Initiate our bitcask backend here
    %%
    %% Default to the current directory and name the top-level "data"
    %% with a sub-directory named bitcask, ensure the directories are
    %% created.
    Backend = pico_riak_bitcask_backend:start(Partition, BCConfig),

    State = #state{ index = Idx, backend = Backend },
    
    %% Construct our vnode worker pool specification.
    Workers = application:get_env(pico_riak, vnode_workers, 1),
    FoldWorkerPool = {pool, pico_riak_vnode_worker, Workers, [Pos]},

    {ok, State, [FoldWorkerPool]}.

%% ===================================================================
%% VNode Command Callbacks
%% ===================================================================
%% @doc Fetch a given key and bucket.
handle_command({get, Bucket, Key}, _Sender, #state{ backend=BackendSt }=State) ->
    {R, DVV, Backend} = pico_riak_bitcask_backend:get(Bucket, Key, BackendSt),
    reply({R, DVV}, State#{ backend = Backend }).

%% @doc Put a brand new object.
handle_command({put, Bucket, Key, Obj}, _Sender, #state{ backend=BackendSt }=State) ->

    %% Create a new DVVSet with our object to store.
    DVV = dvvset:new(Obj),

    %% Set the current actor for use as the server id in the DVVSet.
    DVV1 = dvvset:update(DVV, {State#state.index, node()})

    case catch pico_riak_bitcask_backend:put(Bucket, Key, [], DVVObj, BackendSt) of
        {ok, Backend} ->
            reply(ok, State#{ backend = Backend });
        {error, Error, Backend} ->
            reply({error, Error}, State#{ backend = Backend });
        {'EXIT', Error} ->
            reply({error, Error}, State)

    end;

-spec reply(term(), #state{}) -> {reply, {vnode, integer(), atom(), term()}, #state{}}.
reply(Reply, #state{index=Index}=State) ->
    {reply, {vnode, Index, node(), Reply}, State}.
