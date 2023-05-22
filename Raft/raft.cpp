#include <iostream>
#include <vector>

#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>

#include "locker.hpp"
#include "buttonrpc/buttonrpc.hpp"

#define PORT_ST 10000

using namespace std;

class Operation {
    public:
        string get_cmd(){
            return op + " " + key + " " + value;
        }
        string op;
        string key;
        string value;
        int req_id;
        int client_id;
};

//日志条目，记录做了啥操作，啥参数
struct log_entry{
    log_entry(string op = "", int term=-1): _op(op), _term(term){}
    string _op;
    int _term;
};

//所有需要持久化的项目列表
class Persister{
public:
    vector<log_entry> logs;
    int now_term;
    int vote_for;
};

class peer_node{
    public:
        int _id;
        pair<int, int> _port;   //投票用和其他使用
};

//交互协议定制
// vote
struct req_vote{
    int term;
    int candidate_id;
    int last_log_term;
    int last_log_idx;
};

struct resp_vote{
    int term;
    bool vote_grant;
};

// append
struct req_append{
    int term;
    int leader_id;
    int last_log_term;
    int last_log_idx;
    int leader_commit;
    string send_logs;
    friend Serializer& operator>>(Serializer& in, req_append& req){
        in>>req.term>>req.leader_id>>req.last_log_term>>req.last_log_idx>>req.leader_commit>>req.send_logs;
        return in;
    }
    friend Serializer& operator>>(Serializer& out, req_append& req){
        out<<req.term<<req.leader_id<<req.last_log_term<<req.last_log_idx<<req.leader_commit<<req.send_logs;
        return out;
    }
};

struct resp_append{
    int term;
    bool succ;
    int log_conflict_term;  //  冲突的日志信息
    int log_conflict_idx;
};

struct cmd_resp{
    cmd_resp():cmd_id(-1), term(-1), is_leader(false){}
    int cmd_id;
    int term;
    bool is_leader;
};

class Raft {
    enum RAFT_STATE{
        LEADER=0,
        CANDIDATE,
        FOLLOWER,
    };
    locker _lock;
    cond _cond;
    vector<peer_node> peers;    //所有同胞节点
    Persister persister;
    int _peer_id;
    int dead;

    //----------------需要写盘持久化的数据
    int _cur_term;  //  当前是第几任领袖
    int _voted_for; //  上一次给谁投了票
    vector<log_entry>    _log;   //操作日志
    //----------------需要写盘持久化的数据

    // ??
    vector<int> _next_index;
    vector<int> _match_index;
    int _last_applied;
    int _commit_id;

    int recv_votes;
    int finish_votes;
    int _cur_peer_id;

    RAFT_STATE _state;
    int _leader_id;
    timeval _last_wake_time;
    timeval _last_broadcast_time;
public:
    static void* listen4vote(void* arg);
    static void* listen4append(void* arg);
    static void* log_write_disk_loop(void* arg);
    static void* election_loop(void* arg);
    static void* want2vote(void* arg);
    static void* want2append(void* arg);
    static void* log_apply_loop(void* arg);

    void Make(vector<peer_node> peers, int id);    // 初始化raft
    int get_duration(timeval last);     //
    void set_broadcast_time();      // leader需要发送心跳包，同时要更新发送的时间
    pair<int, bool> get_state();
    resp_vote recv_vote(req_vote req);  //want2vote的对端rpc handler
    resp_append recv_append(req_append req);    //want2append的对端rpc handler
    bool check_log_up2date(int term, int index);    //检查日志是否最新，vote时会用到
    void log_push_back(log_entry log);
    vector<log_entry> string2log_entry(string source);
    cmd_resp start(Operation op);   // 应用层执行i/o唯一入口，只有leader会执行返回结果

    void display_log();

    void serialize();   //序列化
    bool deserialize();
    void write2disk();  //持久话
    void read_from_disk();  // 状态恢复
    bool is_dead();
    void suicide();     //节点自毁
};

void Raft::Make(vector<peer_node> peers, int id)
{
    peers = peers;
    _peer_id = id;
    dead = 0;

    _state = FOLLOWER;
    _cur_term = 0;
    _leader_id = -1;
    _voted_for = -1;
    gettimeofday(&_last_wake_time, NULL);

    recv_votes = 0;
    finish_votes = 0;
    _cur_peer_id = 0;
    
    _last_applied = 0;
    _commit_id = 0;
    _next_index.resize(peers.size(), 1);
    _match_index.resize(peers.size(), 0);

    read_from_disk();

    pthread_t listen_1, listen_2, listen_3;
    pthread_create(&listen_1, NULL, listen4vote, this);
    pthread_detach(listen_1);
    pthread_create(&listen_2, NULL, listen4append, this);
    pthread_detach(listen_2);
    pthread_create(&listen_3, NULL, log_write_disk_loop, this);
    pthread_detach(listen_3);
}

void Raft::read_from_disk()
{
    bool ret = this->deserialize();
    if(!ret) return;
    this->_cur_term = this->persister.now_term;
    this->_voted_for = this->persister.vote_for;

    for(auto& log: this->persister.logs)
    {
        log_push_back(log);
    }
    printf("[%d]'s term: %d, vote for: %d, log.size() = %d", this->_peer_id, this->_cur_term, this->_voted_for, this->_log.size());
}

void Raft::write2disk()
{
    persister.now_term = _cur_term;
    persister.vote_for = _voted_for;
    persister.logs = _log;
    serialize();
}

void Raft::serialize(){
    string str;
    str += to_string(this->persister.now_term) + ":" + to_string(this->persister.vote_for) + ":";
    for(auto& it: persister.logs)
    {
        str += it._op + "," + to_string(it._term) + ".";
    }
    string filename = "persister-" + to_string(_peer_id);
    int fd = open(filename.c_str(), O_RDONLY | O_CREAT, 0664);
    if(fd == -1)
    {
        perror("open");
        exit(-1);
    }
    int len = write(fd, filename.c_str(), filename.length());
    assert(len == filename.length());
}

void* Raft::log_write_disk_loop(void * arg)
{
    Raft* raft = (Raft*) arg;
    while(!raft->is_dead())
    {
        usleep(10000);  //10ms一存盘
        raft->_lock.lock();
        for(int i=raft->_last_applied; i<=raft->_commit_id; i++)
        {
            //TODO
        }
        raft->_last_applied = raft->_commit_id;
        raft->_lock.unlock();
    }
}

int main(int argc, char *argv[])
{
    if(argc < 2)
    {
        std::cout<<"./raft peercnt"<<std::endl;
        exit(-1);
    }
    int peercnt = atoi(argv[1]);
    // peercnt 必须是奇数，不然无法决策
    if(peercnt % 2 == 0)
    {
        std::cout<<"peercnt must be an odd"<<std::endl;
        exit(-1);
    }
    srand((unsigned)time(NULL));
    vector<peer_node> peers(peercnt);
    for(int i=0; i<peercnt; i++)
    {
        peers[i]._id = i;
        peers[i]._port.first = PORT_ST + i;
        peers[i]._port.second = PORT_ST + i + peers.size();
    }

    Raft *raft = new Raft[peers.size()];
    for(int i=0; i<peers.size(); i++)
    {
        raft[i].Make(peers, i);
    }

    /*-----------------------------TEsT-------------------------------------*/
    usleep(400000);
    for(int i=0; i<peers.size(); i++)
    {
        if(raft[i].get_state().second)
        {
            for(int j=0; j<1000; j++)
            {
                Operation op;
                op.op = "put";
                op.key = to_string(j);
                op.value = to_string(j);
                raft[i].start(op);
                usleep(50000);
            }
        }
        else
            continue;
    }

    usleep(400000);
    for(int i=0; i<peers.size(); i++)
    {
        if(raft[i].get_state().second){
            raft[i].suicide();
            break;
        }
    }
    /*-----------------------------TEsT-------------------------------------*/
    while(1);
}