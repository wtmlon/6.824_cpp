#include <iostream>
#include <dlfcn.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <dirent.h>
#include <unordered_map>

#include "./buttonrpc/buttonrpc.hpp"

#define LIB_NAME "./libmrFun.so"
#define OUTPUT_NAME "./MapReduceOut"

struct kv_pair{
    string key;
    string value;
};

typedef vector<kv_pair> (*MapFunc)(kv_pair&);
typedef vector<string> (*ReduceFunc)(vector<kv_pair>&);
typedef vector<string> (*SplitFunc)(char*, int);

int map_task_num = -1, redu_task_num = -1;
MapFunc map_fun = NULL;
ReduceFunc redu_fun = NULL;
SplitFunc split_fun = NULL;
int map_id = 0, redu_id = 0;

pthread_mutex_t gmutex;
pthread_cond_t gcond;

void remove_out_file1()
{
    if(access(OUTPUT_NAME, F_OK) == 0)
    {
        remove(OUTPUT_NAME);
    }
}

void remove_out_file()
{
    string path;
    for(int i=0; i<redu_task_num; i++)
    {
        path = "mr-out-" + to_string(i);
        if (access(path.c_str(), F_OK) == 0)
        {
            remove(path.c_str());
        }
    }
}

void remove_tmp_file()
{
    string path;
    for(int i=0; i<map_task_num; i++)
    {
        for(int j=0; j<redu_task_num; j++)
        {
            path = "mr-" + to_string(i) + "-" + to_string(j);
            if(access(path.c_str(), F_OK) == 0)
            {
                remove(path.c_str());
            }
        }
    }
}

kv_pair get_context(char* file)
{
    int fd = open(file, O_RDONLY);
    int file_len = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    //这里不要用栈空间生成buf，会爆栈
    //char buf[file_len];
    char *buf = new char[file_len];
    memset(buf, 0, sizeof(buf));
    kv_pair kv;
    kv.key = string(file);
    int r = read(fd, buf, file_len);
    if(r != file_len)
    {
        perror("file read");
        exit(-1);
    }
    kv.value += string(buf);
    //int cur = 0;
    //可以直接读取，这里模拟了文件过大，分次读取
    /*while(1)
    {
        int r = read(fd, buf, 10);
        if(r <= 0)
        {
            break;
        }
        std::cout<<"read context: "<<buf<<std::endl;
        cur += r;
        kv.value += string(buf);
        lseek(fd, cur, SEEK_SET);
    }*/
    close(fd);
    return kv;
}

int key_hash(string& key)
{
    int sum = 0;
    for(int i=0; i<key.length(); i++)
    {
        sum += (key[i] - 'a');
    }
    return sum % redu_task_num;
}

void kv2file(kv_pair& kv, int fd)
{
    string tmp = kv.key + ' ';
    int len = write(fd, tmp.c_str(), tmp.length());
    if(len == -1)
    {
        perror("write");
        exit(-1);
    }
    //close(fd);
}

void write_disk(vector<kv_pair>& kv, int mapid)
{
    //std::cout <<"mapid "<<mapid<<std::endl;
    unordered_map<string, int> file2fd;
    for(int i=0; i<kv.size(); i++)
    {
        int rid = key_hash(kv[i].key);
        string path = "mr-" + to_string(map_id) + "-" + to_string(rid);
        //std::cout << path << ' '<<kv[i].key << ' '<<kv[i].value<<endl;
        //std::cout<<to_string(rid)<<std::endl;
        if(file2fd.find(path) == file2fd.end())
        {
            int fd;
            if (access(path.c_str(), F_OK) == 0)
            {
                fd = open(path.c_str(), O_WRONLY | O_APPEND);
            }
            else
            {
                fd = open(path.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
            }
            file2fd[path] = fd;
        }
        kv2file(kv[i], file2fd[path]);
    }
    for(auto &it: file2fd)
    {
        close(it.second);
    }
}

void* map_worker(void* arg)
{
    buttonrpc* client = (buttonrpc*)arg;
    //client.as_client("127.0.0.1", 5555);
    pthread_mutex_lock(&gmutex);
    int mid = map_id++;
    pthread_mutex_unlock(&gmutex);

    while(1)
    {
        if(client->call<bool>("is_map_done").val())
        {
            pthread_cond_signal(&gcond);
            break;
        }

        string task = client->call<string>("assign_map").val();
        if(task == "empty")
        {
            std::cout << "temporay can not fetch task!!" << std::endl;
            continue;
        }
        std::cout<< "fetch the task: "<<task<<"!"<<std::endl;

        char buf[task.length()+1];
        strcpy(buf, task.c_str());
        kv_pair tmp = get_context(buf);
        std::cout << "get context finish" <<endl;
        vector<kv_pair> kv = map_fun(tmp);
        std::cout << "map_fun finish" <<endl;
        write_disk(kv, map_id);
        std::cout << "write disk finish" <<endl;

        std::cout<<"[worker] task complete! tid: "<<task<<" id: "<<map_id<<std::endl;
        client->call<void>("set_map_finish", task);
    }
}

vector<string> get_redu_files_by_rid(string path, int rid)
{
    DIR* dir = opendir(path.c_str());
    vector<string> ret;
    if(dir == NULL)
    {
        std::cout<<"dir not exist: "<<path<<std::endl;
        return ret;
    }
    dirent* entry;
    while((entry = readdir(dir)) != NULL)
    {
        string filename = entry->d_name;
        int filename_len = strlen(entry->d_name);
        int rid_len = to_string(rid).size();
        if(filename[0] != 'm' || filename[1] != 'r' || filename[filename_len-rid_len-1] != '-')
            continue;
        
        string file_rid = filename.substr(filename_len-rid_len, rid_len);
        //std::cout<<file_rid<<"sfskfs"<<std::endl;
        if(file_rid == to_string(rid))
        {
            //std::cout<<filename<<"ssss"<<std::endl;
            ret.push_back(filename);
        }
    }
    closedir(dir);
    return ret;
}

vector<kv_pair> get_reduce_kv(int task)
{
    string path;
    vector<string> all_files = get_redu_files_by_rid(".", task);
    vector<kv_pair> res;
    vector<string> all_tokens;
    map<string, string, std::less<string>> M;
    for(int i=0; i<all_files.size(); i++)
    {
        kv_pair file_content = get_context((char*)all_files[i].c_str());
        vector<string> res_str = split_fun((char*)file_content.value.c_str(), strlen(file_content.value.c_str()));
        all_tokens.insert(all_tokens.end(), res_str.begin(), res_str.end());
    }
    for(int i=0; i<all_tokens.size(); i++)
    {
        if(M.find(all_tokens[i]) == M.end())
            M[all_tokens[i]] = "";
        M[all_tokens[i]] += "1";
    }
    for(auto &it: M)
    {
        kv_pair tmp;
        tmp.key = it.first;
        tmp.value = it.second;
        res.push_back(tmp);
    }
    return res;
}

void write_string2fd(int fd, vector<string>& content)
{
    char buf[50];
    memset(buf, 0, sizeof(buf));
    //std::cout<<content.size()<<std::endl;
    for(int i=0; i<content.size(); i++)
    {
        //std::cout<<content[i]<<std::endl;
        sprintf(buf, (content[i]+"\n").c_str());
        std::cout<<buf<<std::endl;
        int len = write(fd, buf, strlen(buf));
        if(len == -1)
        {
            perror("write");
            exit(-1);
        }
    }
}

void* redu_worker(void* arg)
{
    //buttonrpc* client = (buttonrpc*)arg;
    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    //sleep(5);

    while(1)
    {
        std::cout<<111<<std::endl;
        bool ss = client.call<bool>("is_redu_done").val();
        std::cout<<"ss: "<<ss<<std::endl;
        if(ss)
        {
            //sleep(1);
            return nullptr;
        }
        std::cout<<222<<std::endl;

        int task = client.call<int>("assign_redu").val();
        std::cout<<23<<std::endl;
        if(task == -1)
        {
            continue;
        }
        std::cout<< "pid: " << pthread_self() << " reducer fetch the task: "<<task<<"!"<<std::endl;

        
        vector<kv_pair> kv = get_reduce_kv(task);
        vector<string> res = redu_fun(kv);
        vector<string> str;
        for(int i=0; i<kv.size(); i++)
        {
            str.push_back(kv[i].key + " " + res[i]);
        }
        string file_name = "mr-out-" + to_string(task);
        int fd = open(file_name.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0664);
        write_string2fd(fd, str);
        close(fd);
        std::cout<<"[worker] task complete! tid: "<<task<<" id: "<<map_id<<std::endl;
        client.call<void>("set_redu_finish", task);
    }
}
int main()
{
    void* handle = dlopen(LIB_NAME, RTLD_LAZY);
    if(!handle)
    {
        std::cout<<"dl open fail: "<<dlerror() <<std::endl;
        exit(-1);
    }
    map_fun = (MapFunc)dlsym(handle, "map_f");
    if(!map_fun)
    {
        std::cout<< "map function extract fail: "<<dlerror()<<std::endl;
        exit(-1);
    }
    redu_fun = (ReduceFunc)dlsym(handle, "reduce_f");
    if(!redu_fun)
    {
        std::cout<< "reduce function extract fail: "<<dlerror()<<std::endl;
        exit(-1);
    }
    split_fun = (SplitFunc)dlsym(handle, "split");
    if(!split_fun)
    {
        std::cout<< "split function extract fail: "<<dlerror()<<std::endl;
        exit(-1);
    }

    buttonrpc client;
    client.as_client("127.0.0.1", 5555);
    client.set_timeout(5000);

    map_task_num = client.call<int>("get_map_num").val();
    redu_task_num = client.call<int>("get_redu_num").val();
    cout<<map_task_num<<' '<<redu_task_num<<endl;

    remove_tmp_file();  //清理零食文件
    remove_out_file();  //清理输出文件

    // 生产者消费者模型
    pthread_t map_tids[map_task_num];
    pthread_t redu_tids[redu_task_num];
    for(int i=0; i<map_task_num; i++)
    {
        pthread_create(&map_tids[i], NULL, map_worker, &client);
        pthread_detach(map_tids[i]);
    }

    // mapworker线程全部执行完毕再开启reduce线程
    pthread_mutex_lock(&gmutex);
    pthread_cond_wait(&gcond, &gmutex);
    pthread_mutex_unlock(&gmutex);
    //std::cout<<client.call<bool>("is_map_done").val()<<std::endl;
    //std::cout<<"sdfasdfa"<< client.call<bool>("is_redu_done").val()<<std::endl;
    for(int i=0; i<redu_task_num; i++)
    {
        std::cout<<0120<<std::endl;
        pthread_create(&redu_tids[i], NULL, redu_worker, NULL);
        pthread_detach(redu_tids[i]);
    }
    
    std::cout<<120<<std::endl;
    //死循环卡住，让所有线程都结束才结束，不然main函数结束会清理整个进程
    while(1)
    {
        bool tmp = client.call<bool>("is_redu_done").val();
        std::cout<<"tmp: "<<tmp<<std::endl;
        if(tmp)
        {
            break;
        }
        //放入睡眠，不要一直轮训占用cpu
        sleep(1);
    }
    std::cout<<130<<std::endl;

    //释放资源，删除中间文件
    remove_tmp_file();
    dlclose(handle);
    pthread_mutex_destroy(&gmutex);
    pthread_cond_destroy(&gcond);
    //使用join等到子线程结束，不然client对象会提前析构造成coredump
    //for(int i=0; i<redu_task_num; i++)
    //{
    //    pthread_join(redu_tids[i], NULL);
    //}
    std::cout<<140<<std::endl;

    return 0;
}