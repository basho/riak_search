-module(riak_solr_xml_xform).

-export([xform/1, sax_cb/3]).

-record(state, {
    result,
    stack=[]
}).

-record(partial, {
    name,
    attrs,
    value=[]
}).

xform(Data) ->
    Options = [{event_fun, fun sax_cb/3}, {event_state, #state{}}],
    {ok, {Cmd, Entries}, _} = xmerl_sax_parser:stream(Data, Options),
    {ok, list_to_atom(Cmd), Entries}.

%% @private
%% xmerl_sax_parser callback to parse an XML document into a nested
%% tuple, resulting in something like this:
%%
%% {"add", [
%%    {"doc", [{"id", "ID"}, {"title", "Title"}, ...}
%%    {"doc", [{"id", "ID1"}, {"title", "Title2"}, ...}
%% ]}.
%%
%% Start of a new element, add it to the stack...
sax_cb({startElement, _Uri, Name, _QualName, Attrs}, _Location, State) ->
    Stack = State#state.stack,
    NewStack = [#partial { name=Name, attrs=Attrs }|Stack],
    State#state { stack=NewStack };

%% Got a value, set it to the value of the topmost element in the stack...
sax_cb({characters, Value}, _Location, State) ->
    [Head|Tail] = State#state.stack,
    NewStack = [Head#partial { value = Value }|Tail],
    State#state { stack=NewStack };

%% End of an element, collapse it into the previous item on the stack...
sax_cb({endElement, _Uri, _Name, _QualName}, _Location, State) ->
    [Head0|Tail] = State#state.stack,
    
    %% Special cases, if the partial is named "field" then look for
    %% "name" attribute. (Otherwise, the element name itself is used.)
    case Head0#partial.name == "field" of 
        true -> 
            NewName = find_attr("name", Head0#partial.attrs),
            Head = Head0#partial { name=NewName };
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
