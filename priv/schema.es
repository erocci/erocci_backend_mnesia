#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% Copyright Jean Parpaillon 2014. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.

%% @author Jean Parpaillon <jean.parpaillon@free.fr>

main([]) ->
    io:format("Creates mnesia schema on ~s~n", [application:get_env(mnesia, dir, "")]),
    mnesia:create_schema([node()]).
