all: compile

compile:
	./rebar get-deps
	./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean
