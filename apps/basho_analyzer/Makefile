all:	java_compile erlang_compile

clean: java_clean erlang_clean

java_compile:
	@cd java_src/analysis_server;ant;cd ../..

erlang_compile:
	./rebar compile

java_clean:
	@cd java_src/analysis_server;ant clean;cd ../..

erlang_clean:
	./rebar clean