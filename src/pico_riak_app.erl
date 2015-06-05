-module(pico_riak_app).

-behaviour(application).

-export([start/2, stop/1]).

%% ===================================================================
%% Application Callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case rts_sup:start_link() of
        {ok, Pid} ->
            %% Register pico_riak's vnode.
            ok = riak_core:register(pico_riak, [{vnode_module, pico_riak_vnode}]),

            %% Ensure that all of the vnodes have started before
            %% proceding.
            {ok, Ring} = riak_core_ring_manager:get_my_ring(),
            riak_core_ring_handler:ensure_vnodes_started(Ring),

            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
