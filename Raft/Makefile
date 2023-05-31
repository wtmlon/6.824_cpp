Raft: raft.cpp
	g++ -g -o Raft raft.cpp -lzmq -lpthread
	make clean

rpc_test:
	g++ -g -o rpc_s rpc_test.cpp -lzmq -lpthread
	g++ -g -o rpc_c rpc_test_client.cpp -lzmq -lpthread

.PHONY: clean
clean:
	rm persister*
