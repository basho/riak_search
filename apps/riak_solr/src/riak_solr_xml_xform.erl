%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_solr_xml_xform).

-export([xform/1, sax_cb/3, test/0]).

-ifndef(PRINT).
-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-endif.

-import(riak_search_utils, [to_binary/1, to_atom/1, to_utf8/1]).

-record(state, {
    result,
    stack=[]
}).

-record(partial, {
    name,
    attrs,
    value=[]
}).

test() ->
    Data = [
        {add, [
            {doc, [{id, ["1"]}, {title, ["Title1"]}, {body, ["Body1"]}]},
            {doc, [{id, ["2"]}, {title, ["Title2"]}, {body, ["Body2"]}]},
            {doc, [{id, ["3"]}, {title, ["Title3"]}, {body, ["Body3"]}]}
        ]},
        {add, [
            {doc, [{id, ["4"]}, {title, ["Title4"]}, {body, ["Body4"]}]},
            {doc, [{id, ["5"]}, {title, ["Title5"]}, {body, ["Body5"]}]},
            {doc, [{id, ["6"]}, {title, ["Title6"]}, {body, ["Body6"]}]}
        ]}
    ],
    XML = lists:flatten(xmerl:export_simple(Data, xmerl_xml)),
    xform(XML).

xform(Data) ->
    Options = [{event_fun, fun sax_cb/3}, {event_state, #state{}}],
    {ok, {Cmd, Entries}, _} = xmerl_sax_parser:stream(Data, Options),
    {ok, to_atom(Cmd), Entries}.

%% @private
%% xmerl_sax_parser callback to parse an XML document into a nested
%% tuple, resulting in something like this:
%%
%% {"add", [
%%    {"doc", [{<<"id">>, <<"ID">>}, {<<"title">>, <<"Title">>}, ...}
%%    {"doc", [{<<"id">>, <<"ID1">>}, {<<"title">>, <<"Title2">>}, ...}
%% ]}.
%%
%% Start of a new element, add it to the stack...
sax_cb({startElement, _Uri, Name, _QualName, Attrs}, _Location, State) ->
    Stack = State#state.stack,
    NewStack = [#partial { name=to_binary(Name), attrs=Attrs }|Stack],
    State#state { stack=NewStack };

%% Got a value, set it to the value of the topmost element in the stack...
sax_cb({characters, Value}, _Location, State) ->
    [Head|Tail] = State#state.stack,
    NewStack = [Head#partial { value = to_utf8(Value) }|Tail],
    State#state { stack=NewStack };

%% End of an element, collapse it into the previous item on the stack...
sax_cb({endElement, _Uri, _Name, _QualName}, _Location, State) ->
    [Head0|Tail] = State#state.stack,

    %% Special cases, if the partial is named "field" then look for
    %% "name" attribute. (Otherwise, the element name itself is used.)
    case Head0#partial.name == <<"field">> of
        true ->
            NewName = find_attr("name", Head0#partial.attrs),
            Head = Head0#partial { name=to_binary(NewName) };
        false ->
            Head = Head0
    end,

    %% If we are at the end of the stack, then return the parsed
    %% document. Otherwise, collapse into the previous item on the
    %% stack.
    Value = {Head#partial.name, Head#partial.value},
    case Tail of
        [] ->
            %% Return the parsed doc...
            Value;
        [Parent|Tail1] ->
            NewParent = Parent#partial { value=[Value|Parent#partial.value] },
            State#state { stack=[NewParent|Tail1] }
    end;
sax_cb(_Event, _Location, State) ->
    State.

%% Internal functions
find_attr(Name, Attrs) ->
    case lists:keyfind(Name, 3, Attrs) of
        {_, _, Name, Value} ->
            Value;
        false ->
            undefined
    end.
