#include "master.h"
#include "./buttonrpc/buttonrpc.hpp"

using namespace std;

int main(int argc, char* argv[])
{
    if(argc < 2){
        std::cout << "miss argument, format: ./Master_node xx.txt"<<std::endl;
        exit(-1);
    }
    buttonrpc server;
    server.as_server(5555);
    Master_node master(1, 1);  //13 map work 9 redu work
    master.get_files(argv, argc);
    server.bind("get_map_num", &Master_node::get_mapn, &master);
    server.bind("get_redu_num", &Master_node::get_redn, &master);
    server.bind("assign_map", &Master_node::assign, &master);
    server.bind("assign_redu", &Master_node::assign_reduce, &master);
    server.bind("set_map_finish", &Master_node::set_map_finish, &master);
    server.bind("set_redu_finish", &Master_node::set_redu_finish, &master);
    server.bind("is_map_done", &Master_node::is_done_map, &master);
    server.bind("is_redu_done", &Master_node::done, &master);
    server.run();
    return 0;
}