#include "master.h"

Master_node::Master_node(int map_num, int redu_num): _done(false), _map_num(map_num), _reduce_num(redu_num)
{
    _list.clear();
    finish_maptask.clear();
    finish_redutask.clear();
    map_list.clear();
    redu_list.clear();
    //task_list.clear();
    cur_mapid = 0;
    cur_reduid = 0;
    if(_map_num < 0 || _reduce_num < 0)
    {
        throw std::exception();
    }
    for(int i=0; i<_reduce_num; i++)
    {
        redu_list.push_back(i);
    }
}

void Master_node::get_files(char** files, int index)
{
    for(int i=0; i<index; i++)
    {
        _list.emplace_back(files[i]);
    }
    file_num = index;
}

//unsafe
bool Master_node::is_done_map()
{
    //_lock.lock();     //感觉不用加锁也可以
    return finish_maptask.size() == file_num;
}

void* Master_node::wait_for(void* arg)
{
    char* op = (char*)arg;
    if(*op == 'm')
    {
        sleep(MAP_TIMEOUT);
    }
    else if(*op == 'r')
    {
        sleep(REDU_TIMEOUT);
    }
}

void* Master_node::wait_maptask(void* arg)
{
    Master_node* map = (Master_node*) arg;
    
    void* status;
    pthread_t tid;
    char op = 'm';
    pthread_create(&tid, NULL, wait_for, &op);
    //子线程不结束，就一直阻塞在这
    pthread_join(tid, &status);

    //如果完成队列里面没有这个文件名,代表task fail
    map->_lock.lock();
    //detecting ...
    if(map->finish_maptask.find(map->map_list[map->cur_mapid]) == map->finish_maptask.end())
    {
        string filename = map->map_list[map->cur_mapid];
        std::cout<<"filename: "<< filename <<" timeout!"<<std::endl;
        //两种插入方法，都要测试一下
        //char tmp[map->map_list[map->cur_mapid].length()+1];
        //strcpy(tmp, map->map_list[map->cur_mapid].c_str());
        //map->_list.push_back(tmp);

        map->_list.push_back((char*)filename.c_str());
        map->cur_mapid++;
        map->_lock.unlock();
        return NULL;
    }
    std::cout<<"filename: "<< map->map_list[map->cur_mapid] <<" finish at "<< map->cur_mapid<<"!" <<std::endl;
    map->cur_mapid++;
    map->_lock.unlock();
}

void Master_node::wait_map(string filename)
{
    _lock.lock();
    map_list.push_back(filename);
    _lock.unlock();

    //创建新线程执行rpc调用监督map worker工作，记得传入this参数
    pthread_t tid;
    pthread_create(&tid, NULL, wait_maptask, this);
    pthread_detach(tid);
}

string Master_node::assign()
{
    if(is_done_map())return "empty";
    if(!_list.empty())
    {
        _lock.lock();
        char* task = _list.back();
        _list.pop_back();
        _lock.unlock();
        wait_map(string(task));
        return string(task);
    }
    return "empty";
}

bool Master_node::done()
{
    return finish_redutask.size() == _reduce_num;
}

void* Master_node::wait_reducetask(void* arg)
{
    Master_node* m = (Master_node*) arg;
    pthread_t tid;
    void* thread_return;
    char op = 'r';
    pthread_create(&tid, NULL, wait_for, (void*)&op);
    pthread_join(tid, &thread_return);

    m->_lock.lock();
    if(m->finish_redutask.find(m->redu_list[m->cur_reduid]) == m->finish_redutask.end())
    {
        m->task_list.push_back(m->redu_list[m->cur_reduid]);
        std::cout<<"reduce task: "<<m->redu_list[m->cur_reduid]<< " timeout!"<<std::endl;
        m->cur_reduid++;
        m->_lock.unlock();
        return NULL;
    }
    std::cout<<"reduce task: "<<m->redu_list[m->cur_reduid]<< " finish!"<<std::endl;
    m->cur_reduid++;
    m->_lock.unlock();
    return NULL;
}

void Master_node::wait_redu(int rid)
{
    _lock.lock();
    redu_list.push_back(rid);
    _lock.unlock();
    pthread_t tid;
    pthread_create(&tid, NULL, wait_reducetask, (void*)this);
    pthread_detach(tid);
}

int Master_node::assign_reduce()
{
    if(done())return -1;
    if(!redu_list.empty())
    {
        _lock.lock();
        int rid = redu_list.back();
        redu_list.pop_back();
        _lock.unlock();
        wait_redu(rid);
        return rid;
    }
    return -1;
}

void Master_node::set_map_finish(string m)
{
    _lock.lock();
    if(finish_maptask.find(m) == finish_maptask.end())
        finish_maptask[m] = 1;
    _lock.unlock();
}

void Master_node::set_redu_finish(int r)
{
    _lock.lock();
    if(finish_redutask.find(r) == finish_redutask.end())
        finish_redutask[r] = 1;
    _lock.unlock();
}