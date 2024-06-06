%%%-------------------------------------------------------------------
%% @doc mqtt_retain_bm public API
%% @end
%%%-------------------------------------------------------------------

-module(mqtt_retain_bm_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    mqtt_retain_bm_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
