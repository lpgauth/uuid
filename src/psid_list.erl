% psid_list:init().
% ets:info(psid_lists).
% psid_list:delta_varint_list_decode(ets:lookup_element(psid_lists, ets:first(psid_lists), 2)).
% psid_list:bench().

-module(psid_list).

-export([
    init/0,
    bench/0,
    delta_varint_list_encode/1,
    delta_varint_list_decode/1
]).

-define(N, 200).
-define(P, 1000).
-define(TID, psid_lists).

%% public
bench() ->
    Uid = ets:first(?TID),
    {Diff, _Return} = timer:tc(fun () ->
        lists:foreach(fun (_) ->
            Bin = ets:lookup_element(?TID, Uid, 2),
            delta_varint_list_decode(Bin)
        end, lists:seq(1, 10000))
    end),
    Diff / 10000.0.

init() ->
    ?TID = ets:new(?TID, [public, named_table, {read_concurrency, true}]),
    UidState = uuid:new(self()),

    spawn(fun() ->
        lists:foreach(fun (_) ->
            {Uuid, _} = uuid:get_v1(UidState),
            ets:insert(?TID, {Uuid, memberships()})
        end, lists:seq(1, ?P))
    end).

%% private
memberships() ->
    Rand = [rand:uniform(10000) || _ <- lists:seq(1, ?N)],
    delta_varint_list_encode(lists:usort(Rand)).

%% codecs
delta_varint_list_encode([]) ->
    <<>>;
delta_varint_list_encode([H | T]) ->
    List = [varint_encode(H) | delta_varint_list_encode(H, T)],
    iolist_to_binary(List).

delta_varint_list_encode(H1, [H2]) ->
    [varint_encode(H2 - H1)];
delta_varint_list_encode(H1, [H2 | T]) ->
    [varint_encode(H2 - H1) | delta_varint_list_encode(H2, T)].

delta_varint_list_decode(B) ->
    case varint_list_decode(B) of
        [] -> [];
        [N0 | Deltas] ->
            [N0 | delta_decode(N0, Deltas)]
    end.

delta_decode(_N, []) -> [];
delta_decode(N, [D0 | Deltas]) ->
    N0 = N + D0,
    [N0 | delta_decode(N0, Deltas)].

varint_encode(I) ->
    varint_encode(I, []).

varint_encode(I, Acc) when I =< 16#7f ->
    iolist_to_binary(lists:reverse([I | Acc]));
varint_encode(I, Acc) ->
    LastSevenBits = (I - ((I bsr 7) bsl 7)),
    OtherBits = (I bsr 7),
    NewBit = LastSevenBits bor 16#80,
    varint_encode(OtherBits, [NewBit | Acc]).

varint_decode(<<1:1, Byte:7, Rest/binary>>, Value, Shift) ->
    varint_decode(Rest, Value bor (Byte bsl Shift), Shift + 7);
varint_decode(<<Byte:8, Rest/binary>>, Value, Shift) ->
    {Value bor (Byte bsl Shift), Rest}.

varint_list_decode(B) ->
    lists:reverse(varint_list_decode_rev(B)).
varint_list_decode_rev(B) ->
    varint_list_decode(B, []).
varint_list_decode(<<>>, Acc) ->
    Acc;
varint_list_decode(Binary, Acc) ->
    {Value, Rest} = varint_decode(Binary, 0, 0),
    varint_list_decode(Rest, [Value | Acc]).

% test_delta_varint_list_decode(<<16#00, 16#01, 16#01, 16#01, 16#01>>,
%   [0, 1, 2, 3, 4]).

% test_varint_list_decode(<<16#f1, 16#08, 16#80, 16#80, 16#40>>,
%   [1137, 1048576]).