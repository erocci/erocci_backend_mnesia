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

-export([models/1,
         get/2,
         create/4,
         create/5,
         update/3,
         link/4,
         unlink/4,
         action/4,
         delete/2,
         mixin/4,
         unmixin/3,
         collection/5]).

-export([mnesia_disc_copies/1]).

-define(BASE_SCHEME, <<"http://schemas.erocci.ow2.org/">>).

-define(REC, ?MODULE).
-define(COLLECTION, erocci_backend_mnesia_collection).
-define(LINKS, erocci_backend_mnesia_links).

-record(?REC, {location, entity, owner, group, serial}).
-record(?COLLECTION, {category, location, usermixin}).
-record(?LINKS, {link, type, endpoint}).

-define(TABLES(Copies), [{?REC, Copies ++ [ {attributes, record_info(fields, ?REC)} ]},
                         {?COLLECTION, Copies ++ [ {attributes, record_info(fields, ?COLLECTION)},
                                                   {type, bag} ]},
                         {?LINKS, Copies ++ [ {attributes, record_info(fields, ?LINKS)},
                                              {type, bag} ]}]
       ).

-type state() :: [occi_extension:t()].

%%% @doc Returns nodes on which a mnesia schema must be created
%%% @todo Generalize to all components ?
%%% @end
mnesia_disc_copies(_) ->
  application:load(erocci_backend_mnesia),
  Copies = lists:foldl(fun validate_copies/2, [], application:get_env(erocci_backend_mnesia, copies, [])),
  lists:foldl(fun ({disc_copies, Nodelist}, Acc) ->
                  Nodelist ++ Acc;
                  ({disc_only_copies, Nodelist}, Acc) ->
                  Nodelist ++ Acc;
                  (_, Acc) ->
                  Acc
              end, [], Copies).


%%%===================================================================
%%% occi_backend callbacks
%%%===================================================================
-spec init([]) -> {ok, erocci_backend:capability(), state()} | {error, term()}.
init(Opts) ->
  {ok, _} = application:ensure_all_started(erocci_backend_mnesia),
  Copies = lists:foldl(fun validate_copies/2, [], application:get_env(erocci_backend_mnesia, copies, [])),
  case init_schema(missing_tables(?TABLES(Copies))) of
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


models(S) ->
  {{ok, S}, S}.


get(Location, S) ->
  ?info("[~s] get(~s)", [?MODULE, Location]),
  Get = fun () ->
            case mnesia:read(?REC, Location) of
              [] ->
                {error, not_found};
              [{?REC, _, Entity, Owner, Group, Serial}] ->
                case occi_type:type(Entity) of
                  resource ->
                    get_links(Entity, Owner, Group, Serial);
                  _ ->
                    {ok, Entity, Owner, Group, Serial}
                end
            end
        end,
  transaction(Get, S).


create(Location, Entity, Owner, Group, S) ->
  ?info("[~s] create(~s)", [?MODULE, Location]),
  Fun =  fun () ->
             Node = case mnesia:read(?REC, Location) of
                      [] ->
                        {?REC, Location, Entity, Owner, Group, integer_to_binary(1)};
                      [{?REC, _, _, Owner, _, Serial}] ->
                        case delete_links(occi_type:type(Entity), Location) of
                          ok ->
                            {?REC, Location, Entity, Owner, Group, incr(Serial)};
                          {error, _}=Err ->
                            Err
                        end;
                      [{?REC, _, _, _OtherOwner, _, _}] ->
                        {error, conflict}
                    end,
             case gen_create(Node) of
               {error, _}=Err2 ->
                 Err2;
               ok ->
                 {ok, Node#?REC.entity, Node#?REC.serial}
             end
         end,
  transaction(Fun, S).


create(Entity, Owner, Group, S) ->
  Location = case occi_entity:get(<<"occi.core.id">>, Entity) of
               undefined ->
                 uuid:uuid_to_string(uuid:get_v4(), binary_standard);
               V ->
                 V
             end,
  Entity2 = occi_entity:location(Location, Entity),
  ?info("[~s] create(~s)", [?MODULE, Location]),
  Node = {?REC, Location, Entity2, Owner, Group, integer_to_binary(1)},
  transaction(fun () ->
                  ok = gen_create(Node),
                  {ok, Location, Node#?REC.entity, Node#?REC.serial}
              end, S).


update(Location, Attributes, S) ->
  ?info("[~s] update(~s)", [?MODULE, Location]),
  Update = fun () ->
               [Node] = mnesia:wread({?REC, Location}),
               Entity = occi_entity:update(Attributes, client, Node#?REC.entity),
               Node1 = Node#?REC{ entity=Entity,
                                  serial=incr(Node#?REC.serial) },
               ok = mnesia:write(Node1),
               {ok, Entity, Node1#?REC.serial}
           end,
  transaction(Update, S).


link(Location, Type, LinkId, S) ->
  ?info("[~s] link(~s, ~s, ~s)", [?MODULE, Location, Type, LinkId]),
  Link = fun () ->
             Rec = #?LINKS{ link=LinkId, type=Type, endpoint=Location },
             mnesia:write(Rec)
         end,
  transaction(Link, S).


unlink(Location, Type, LinkId, S) ->
  ?info("[~s] unlink(~s, ~s, ~s)", [?MODULE, Location, Type, LinkId]),
  Unlink = fun () ->
               Obj = #?LINKS{ endpoint=Location, type=Type, link=LinkId },
               mnesia:delete(Obj)
           end,
  transaction(Unlink, S).


action(Location, _ActionId, _Attributes, S) ->
  ?info("[~s] invoke(~s, ~p)", [?MODULE, Location, _ActionId]),
  %% Storage only, not supported
  [Node] = mnesia:activity(transaction, fun () -> mnesia:read(?REC, Location) end),
  {{ok, Node#?REC.entity, Node#?REC.serial}, S}.


delete(Location, S) ->
  ?info("[~s] delete(~s)", [?MODULE, Location]),
  Delete = fun () ->
               [Node] = mnesia:wread({?REC, Location}),
               Kind = occi_entity:kind(Node#?REC.entity),
               Mixins = occi_entity:mixins(Node#?REC.entity),
               ok = lists:foreach(fun (CategoryId) ->
                                      Match1 = #?COLLECTION{ category=CategoryId, location=Location, _='_' },
                                      [Obj1] = mnesia:match_object(Match1),
                                      mnesia:delete_object(Obj1)
                                  end, [ Kind | Mixins ]),
               mnesia:delete({?REC, Location})
           end,
  transaction(Delete, S).


mixin(Location, Mixin, _Attributes, S) ->
  ?info("[~s] mixin(~s, ~p)", [?MODULE, Location, occi_mixin:id(Mixin)]),
  AddMixin = fun () ->
                 [Node] = mnesia:wread({?REC, Location}),
                 Node1 = Node#?REC{ entity=occi_entity:add_mixin(Mixin, Node#?REC.entity),
                                    serial=incr(Node#?REC.serial) },
                 ok = mnesia:write(Node1),
                 ok = mnesia:write({?COLLECTION, occi_mixin:id(Mixin), Location, occi_mixin:tag(Mixin)}),
                 {ok, Node1#?REC.entity, Node1#?REC.serial}
             end,
  transaction(AddMixin, S).


unmixin(Location, Mixin, S) ->
  ?info("[~s] unmixin(~s, ~p)", [?MODULE, Location, occi_mixin:id(Mixin)]),
  Unmixin = fun () ->
                [Node] = mnesia:wread({?REC, Location}),
                Node1 = Node#?REC{ entity=occi_entity:rm_mixin(Mixin, Node#?REC.entity),
                                   serial=incr(Node#?REC.serial) },
                ok = mnesia:write(Node1),
                Objs = mnesia:match_object(#?COLLECTION{ category=occi_mixin:id(Mixin),
                                                         location=Location, _='_' }),
                lists:foreach(fun (X) ->
                                  mnesia:delete_object(X)
                              end, Objs),
                {ok, Node#?REC.entity, Node1#?REC.serial}
            end,
  transaction(Unmixin, S).


%% @todo memorize cursors ?, implements filters
%% @end
collection(Id, _Filter, Start, undefined, S) ->
  collection(Id, _Filter, Start, all_remaining, S);

collection(Id, _Filter, Start, Number, S) ->
  ?info("[~s] collection(~p, ~b, ~p)", [?MODULE, Id, Start, Number]),
  Collection = fun () ->
                   QH = qlc:q([ Node#?REC.location ||
                                Node <- mnesia:table(?REC),
                                Coll <- mnesia:table(?COLLECTION),
                                Coll#?COLLECTION.category =:= Id,
                                Node#?REC.location =:= Coll#?COLLECTION.location ]),
                   QC = qlc:cursor(QH),
                   case Start of
                     1 -> ok;
                     _ -> _Trash = qlc:next_answers(QC, Start-1)
                   end,
                   {ok, qlc:next_answers(QC, Number), undefined}
               end,
  transaction(Collection, S).


%%%===================================================================
%%% Internal functions
%%%===================================================================
get_links(Resource, Owner, Group, Serial) ->
  Match = #?LINKS{ endpoint=occi_resource:location(Resource), _='_' },
  Links = lists:map(fun (#?LINKS{ link=Location }) ->
                        Location
                    end, mnesia:match_object(Match)),
  {ok, occi_resource:links(Links, Resource), Owner, Group, Serial}.


delete_links(Type, Location) ->
  LinkMatch = case Type of
                resource ->
                  #?LINKS{ endpoint=Location, _='_' };
                link ->
                  #?LINKS{ link=Location, _='_' }
              end,
  lists:foreach(fun (Obj) ->
                    mnesia:delete_object(Obj)
                end, mnesia:match_object(LinkMatch)).


gen_create({error, _}=Err) ->
  Err;

gen_create(Node) ->
  ok = mnesia:write(Node),
  ok = mnesia:write({?COLLECTION, occi_entity:kind(Node#?REC.entity), Node#?REC.location, false}),
  ok = lists:foreach(fun (MixinId) ->
                         Mixin = occi_models:category(MixinId),
                         Tag = occi_mixin:tag(Mixin),
                         mnesia:write({?COLLECTION, MixinId, Node#?REC.location, Tag})
                     end, occi_entity:mixins(Node#?REC.entity)),
  ok.


init_schema(no_schema) ->
  {error, schema};

init_schema([]) ->
  mnesia:wait_for_tables([?REC, ?COLLECTION], 10000);

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
  QH = qlc:q([ Id || {_, Id, _, Tag} <- mnesia:table(?COLLECTION), Tag =:= true ], {unique, true}),
  MixinIds = mnesia:activity(transaction, fun () -> qlc:eval(QH) end),
  Ext2 = lists:foldl(fun ({Scheme, Term}, Acc) ->
                         ?debug("add mixin ~p", [{Scheme, Term}]),
                         Mixin = occi_mixin:new(Scheme, Term),
                         occi_extension:add_category(Mixin, Acc)
                     end, Ext, MixinIds),
  {ok, [], [Ext2]}.


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
  try mnesia:activity(transaction, Fun) of
      Res ->
	    {Res, S}
  catch exit:Reason ->
	    {{error, Reason}, S}
  end.


validate_copies({Opt, Value}, Acc) when disc_copies =:= Opt
                                        orelse disc_only_copies =:= Opt
                                        orelse ram_copies =:= Opt ->
  Nodelist = case Value of
               local -> [node()];
               remote -> nodes();
               all -> [ node() | nodes() ];
               List -> List
             end,
  [ {Opt, Nodelist} | Acc ];

validate_copies(_, Acc) ->
  Acc.
