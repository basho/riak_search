all: get-deps compile

get-deps:
	./rebar get-deps

compile:
	./rebar compile
	@cd java_src;make

clean:
	./rebar clean
	@cd java_src;make clean

distclean: clean
	./rebar delete-deps