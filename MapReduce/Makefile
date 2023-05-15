mr_fun:
	g++ -fpic -c map_reduceFun.cpp
	g++ -shared -o libmrFun.so map_reduceFun.o
master:
	g++ -fPIC -c master.cpp
	#ar rcs libmaster.a master.o
	g++ -shared -o libmaster.so master.o
mnode: mr_fun master
	g++ -o master_node master_node.cpp -L./ -lmaster -lzmq
.PHONY: clean
clean:
	rm *.o *.so *.a
