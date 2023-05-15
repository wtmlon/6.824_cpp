#include <iostream>

#include <list>
#include <unordered_map>
#include <string>
#include <vector>

#include <unistd.h>
#include <string.h>

#include "locker.hpp"

#define MAP_TIMEOUT 3
#define REDU_TIMEOUT 5
using namespace std;

class Master_node{
    public:
        static void* wait_maptask(void *arg);
        static void* wait_reducetask(void * arg);
        static void* wait_for(void* arg);

        Master_node(int map_num=8, int reduce_num=8);

        void get_files(char** file, int index);

        int get_mapn(){return _map_num;}
        int get_redn(){return _reduce_num;}

        string assign();    //给worker分配任务，rpc调用
        int assign_reduce();    //分配reduce任务，rpc调用
        void set_map_finish(string m);
        void set_redu_finish(int r);

        bool is_done_map();     //检验所有map是否完成, rpc
        void wait_map(string filename);
        void wait_redu(int redu_id);
        bool done();            //检验reduce是否完成
        bool get_final_stat(){  //和上函数差不多，考虑删除
            return _done;
        }
    private:
        int _map_num, _reduce_num;
        bool _done;
        list<char*> _list;  //map工作队列
        locker _lock;       //自己实现的锁，保护队列安全性
        int file_num;

        unordered_map<string, int> finish_maptask;  //存放所有的map完成task, string是文件名
        unordered_map<int, int> finish_redutask;    //存放所有的redu完成task，int为redu_id

        vector<int> task_list;  //所有redu工作队列
        vector<int> redu_list;  //redu task工作队列
        vector<string> map_list;    //map task工作队列
        
        int cur_mapid, cur_reduid;

};