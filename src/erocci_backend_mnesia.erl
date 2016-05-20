%%% @author Jean Parpaillon <jean.parpaillon@free.fr>
%%% @copyright (c) 2013-2016 Jean Parpaillon
%%% 
%%% This file is provided to you under the license described
%%% in the file LICENSE at the root of the project.
%%%
%%% You can also download the LICENSE file from the following URL:
%%% https://github.com/erocci/erocci/blob/master/LICENSE
%%% 
%%% @doc
%%%
%%% @end
%%% Created :  1 Jul 2013 by Jean Parpaillon <jean.parpaillon@free.fr>
-module(erocci_backend_mnesia).

-behaviour(erocci_backend).

-include_lib("stdlib/include/qlc.hrl").
-include_lib("erocci_core/include/erocci_log.hrl").

%% occi_backend callbacks
-export([init/1,
         terminate/1]).

-export([model/1,
	 get/2,
	 create/4,
	 create/5,
	 update/3,
	 action/3,
	 delete/2,
	 mixin/3,
	 unmixin/3,
	 collection/5]).

-define(BASE_SCHEME, <<"http://schemas.erocci.ow2.org/">>).

-define(REC, ?MODULE).
-define(COLLECTION, erocci_backend_mnesia_collection).

-record(?REC, {id, entity, owner, group, serial}).
-record(?COLLECTION, {category, id, usermixin}).

-define(TABLES, [{?REC, [{disc_copies, [node()]},
			 {attributes, record_info(fields, ?REC)}]},
		 {?COLLECTION, [{disc_copies, [node()]},
				{attributes, record_info(fields, ?COLLECTION)}]}]).

-type state() :: occi_extension:t().

%%%===================================================================
%%% occi_backend callbacks
%%%===================================================================
-spec init([]) -> {ok, erocci_backend:capability(), state()} | {error, term()}.
init(Opts) ->
    {ok, _} = application:ensure_all_started(erocci_backend_mnesia),
    case init_schema(missing_tables(?TABLES)) of
	ok ->
	    init_model(proplists:get_value(schema, Opts, []), Opts);
	{error, schema} ->
	    ?error("###~n"
		   "### You must first create Mnesia database files with:~n"
		   "### ~s/schema.es~n"
		   "###~n", [code:priv_dir(erocci_backend_mnesia)]),
	    timer:sleep(1000),
	    erlang:halt(1)
    end.


terminate(_S) ->
    application:unload(erocci_backend_mnesia),
    ok.


model(S) ->
    {{ok, S}, S}.


get(Id, S) ->
    ?info("[~s] get(~s)", [?MODULE, Id]),
    Get = fun () ->
		  case mnesia:read(?REC, Id) of
		      [] -> 
			  throw(not_found);
		      [{?REC, _, Entity, Owner, Group, Serial}] ->
			  {ok, Entity, Owner, Group, Serial}
		  end
	  end,
    transaction(Get, S).


create(Id, Entity, Owner, Group, S) ->
    ?info("[~s] create(~s)", [?MODULE, Id]),
    Fun =  fun () ->
		   Node = case mnesia:read(?REC, Id) of
			      [] ->
				  {?REC, Id, Entity, Owner, Group, integer_to_binary(1)};
			      [{?REC, _, _, Owner, _, Serial}] ->
				  {?REC, Id, Entity, Owner, Group, incr(Serial)};
			      [{?REC, _, _, _OtherOwner, _, _}] ->
				  throw(conflict)
			  end,
		   gen_create(Node)
	   end,
    transaction(Fun, S).
    

create(Entity, Owner, Group, S) ->
    ?info("[~s] create(~s)", [?MODULE, occi_entity:location(Entity)]),
    Id = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    Node = {?REC, Id, occi_entity:id(Id, Entity), Owner, Group, integer_to_binary(1)},
    transaction(fun () -> gen_create(Node) end, S).


update(Actual, Attributes, S) ->
    ?info("[~s] update(~s)", [?MODULE, occi_entity:id(Actual)]),
    Update = fun () ->
		     Id = occi_entity:id(Actual),
		     [Node] = mnesia:wread({?REC, Id}),
		     Entity = occi_entity:update(Attributes, client, Actual),
		     ok = mnesia:write(Node#?REC{ entity=Entity,
						  serial=incr(Node#?REC.serial) }),
		     {ok, Entity}
	     end,
    transaction(Update, S).


action(_Invoke, Entity, S) ->
    ?info("[~s] invoke(~s)", [?MODULE, occi_entity:id(Entity)]),
    %% Storage only, not supported
    {{ok, Entity}, S}.


delete(Id, S) ->
    ?info("[~s] delete(~s)", [?MODULE, Id]),
    Delete = fun () ->
		     mnesia:delete({?REC, Id})
	     end,
    transaction(Delete, S).


mixin(Mixin, Actual, S) ->
    ?info("[~s] mixin(~s)", [?MODULE, occi_entity:id(Actual)]),
    Mixin = fun () ->
		    Id = occi_entity:id(Actual),
		    [Node] = mnesia:wread({?REC, Id}),
		    Entity = occi_entity:add_mixin(Mixin, Actual),
		    ok = mnesia:write(Node#?REC{ entity=Entity,
						 serial=incr(Node#?REC.serial) }),
		    ok = mnesia:write({?COLLECTION, occi_mixin:id(Mixin), Id, occi_mixin:tag(Mixin)}),
		    {ok, Entity}
	    end,
    transaction(Mixin, S).


unmixin(Mixin, Actual, S) ->
    ?info("[~s] unmixin(~s)", [?MODULE, occi_entity:id(Actual)]),
    Unmixin = fun () ->
		      Id = occi_entity:id(Actual),
		      [Node] = mnesia:wread({?REC, Id}),
		      Entity = occi_entity:rm_mixin(Mixin, Actual),
		      mnesia:write(Node#?REC{ entity=Entity,
					      serial=incr(Node#?REC.serial) }),
		      mnesia:delete_object(#?COLLECTION{ category=occi_mixin:id(Mixin), id=Id, _='_' })
	    end,
    transaction(Unmixin, S).


%% @todo memorize cursors ?, implements filters
%% @end
collection(Id, _Filter, Start, Number, S) ->
    ?info("[~s] collection(~p, ~b, ~p)", [?MODULE, Id, Start, Number]),
    Collection = fun () ->
			 QH = qlc:q([ { Node#?REC.entity, Node#?REC.owner, Node#?REC.group } ||
					Node <- mnesia:table(?REC),
					Coll <- mnesia:table(?COLLECTION),
					Coll#?COLLECTION.category =:= Id,
					Node#?REC.id =:= Coll#?COLLECTION.id ]),
			 QC = qlc:cursor(QH),
			 case Start of
			     0 -> ok;
			     _ -> _Trash = qlc:next_answers(QC, Start-1)
			 end,
			 {ok, qlc:next_answers(QC, Number), undefined}
		 end,
    transaction(Collection, S).


%%%===================================================================
%%% Internal functions
%%%===================================================================
gen_create(Node) ->
    ok = mnesia:write(Node),
    ok = mnesia:write({?COLLECTION, occi_entity:kind(Node#?REC.entity), Node#?REC.id, false}),
    lists:foreach(fun (MixinId) ->
			  Mixin = occi_models:category(MixinId),
			  Tag = occi_mixin:tag(Mixin),
			  mnesia:write({?COLLECTION, MixinId, Node#?REC.id, Tag})
		  end, occi_entity:mixins(#?REC.entity)).


init_schema(no_schema) ->
    {error, schema};

init_schema([]) ->
    init_mnesia:wait_for_tables([?REC, ?COLLECTION], 10000);

init_schema(Missing) ->
    init_tables(Missing).


init_tables([]) ->
    ok;

init_tables([{Name, TabDef} | Tables]) ->
    case mnesia:create_table(Name, TabDef) of
        {atomic, ok} ->
            ?debug("mnesia: created table: ~p~n", [Name]),
	    mnesia:wait_for_tables(Name, 10000),
            init_tables(Tables);
        {aborted, {already_exists, Name}} ->
            ?debug("mnesia: table ~p already exists~n", [Name]),
            init_tables(Tables);
        {aborted, Reason} ->
            {error, Reason}
    end.


init_model([], _Opts) ->
    {error, no_schema};

init_model({priv_dir, Path}, Opts) when is_list(Path); is_binary(Path) ->
    ?debug("Load OCCI schema from priv dir: ~s", [Path]),
    Fullpath = filename:join([code:priv_dir(erocci_backend_mnesia), Path]),
    init_user_mixins(occi_rendering:parse_file(Fullpath, occi_extension), Opts);

init_model(Path, Opts) when is_list(Path); is_binary(Path) ->
    ?debug("Load OCCI schema from: ~s", [Path]),
    init_user_mixins(occi_rendering:parse_file(Path, occi_extension), Opts);

init_model({extension, Scheme}, Opts) ->
    ?debug("Load OCCI extension: ~s", [Scheme]),
    Uuid = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    BackendScheme = << ?BASE_SCHEME/binary, Uuid/binary >>,
    Ext = occi_extension:add_import(Scheme, occi_extension:new(BackendScheme)),
    init_user_mixins(Ext, Opts).


init_user_mixins(Ext, _Opts) ->
    %% TODO
    {ok, [], Ext}.


missing_tables(Tables) ->
    case mnesia:table_info(schema, disc_copies) of
	[] -> 
	    no_schema;
	_Nodes ->
	    missing_tables(Tables, [])
    end.


missing_tables([], Acc) ->
    Acc;

missing_tables([ {Id, _}=Def | Tables ], Acc) ->
    try mnesia:table_info(Id, disc_copies) of
	[] -> missing_tables(Tables, [ Def | Acc ]);
	_Nodes -> missing_tables(Tables, Acc)
    catch exit:{aborted, {no_exists, _, _}} ->
	    missing_tables(Tables, [ Def | Acc ])
    end.	    


incr(Serial) when is_binary(Serial) ->
    integer_to_binary(binary_to_integer(Serial) + 1).


transaction(Fun, S) ->
    case mnesia:transaction(Fun) of
	{atomic, Ok} ->
	    {Ok, S};
	{aborted, Error} ->
	    {{error, Error}, S}
    end.
