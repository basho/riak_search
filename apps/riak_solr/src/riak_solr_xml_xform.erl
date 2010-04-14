-module(riak_solr_xml_xform).

-export([xform/2, sax_cb/3]).

-record(xml_state, {docs=[],
                cmd,
                current_element,
                current_field,
                current_doc}).

xform(Index, Data) ->
    {ok, ParsedData, _} = xmerl_sax_parser:stream(Data, [{event_fun, fun sax_cb/3},
                                                         {event_state, #xml_state{}}]),
    [{cmd, ParsedData#xml_state.cmd},
     {docs, ParsedData#xml_state.docs},
     {index, Index}].

sax_cb({startElement, _Uri, Cmd, _QualName, _Attrs}, _Location, State) when Cmd =:= "add";
                                                                            Cmd =:= "del" ->
    State#xml_state{cmd=list_to_atom(Cmd)};
sax_cb({startElement, _Uri, "field", _QualName, Attrs}, _Location, State) ->
    State#xml_state{current_field=find_attr("name", Attrs)};
sax_cb({characters, Text}, _Location, #xml_state{current_field=CurrField, current_doc=CurrDoc}=State) ->
    State#xml_state{current_doc=dict:store(CurrField, Text, CurrDoc)};
sax_cb({startElement, _Uri, "doc", _QualName, _Attrs}, _Location, State) ->
    State#xml_state{current_doc=dict:new()};
sax_cb({endElement, _Uri, "doc", _QualName}, _Location, #xml_state{docs=Docs, current_doc=Curr}=State) ->
    State#xml_state{docs=[Curr|Docs], current_doc=undefined};

sax_cb(_Event, _Location, State) ->
    State.

%% Internal functions
find_attr(_Name, []) ->
    undefined;
find_attr(Name, [{_, _, Name, Value}|_]) ->
    Value;
find_attr(Name, [_|T]) ->
    find_attr(Name, T).
