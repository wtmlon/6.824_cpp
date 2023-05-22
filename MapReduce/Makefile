# map reduce的实现功能
mr_fun:
	g++ -fpic -c map_reduceFun.cpp
	g++ -shared -o libmrFun.so map_reduceFun.o
# master节点动态库
master:
	g++ -fPIC -c master.cpp
	#ar rcs libmaster.a master.o
	g++ -shared -o libmaster.so master.o
# master node main函数
mnode: mr_fun master
	g++ -o master_node -g master_node.cpp -L./ -lmaster -lzmq
# worker main函数
wnode: mr_fun
	g++ -o worker_node -g worker.cpp -lzmq -lpthread -ldl
.PHONY: all
all: mnode wnode

.PHONY: clean
clean:
	rm *.o *.so *.a
