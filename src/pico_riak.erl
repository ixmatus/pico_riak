-module(pico_riak).

-include_lib("riak_core/include/riak_core_vnode.hrl").

-type key() :: term().
-type ctx() :: dvvset:vector().

%%%===================================================================
%%% Public API
%%%===================================================================

-spec ping() -> any().
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, pico_riak),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, pico_riak_vnode_master).

-spec get(key()) -> 'ok'.
get(Key) ->
    ok.

-spec put(key(), term()) -> 'ok'.
put(Key, Val) ->
    ok.

-spec del(key()) -> 'ok'.
del(Key) ->
    ok.

-spec list_keys() -> [key()].
list_keys() ->
    ok.
