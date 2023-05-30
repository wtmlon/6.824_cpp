#include <iostream>
#include <vector>

#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>

#include "locker.hpp"
#include "buttonrpc-master/buttonrpc.hpp"

#define PORT_ST 10000
#define HEARTBEAT_INTERVAL 100000

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
    friend Serializer& operator<<(Serializer& out, req_append& req){
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

    // 每个从节点下一个该同步的idx
    //每个从节点已经同步好的idx 
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
    static void* log_process_loop(void* arg);
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

bool Raft::is_dead()
{
    return dead == 1;
}

cmd_resp Raft::start(Operation op)
{
    cmd_resp ret;
    _lock.lock();
    RAFT_STATE state = _state;
    if(state != LEADER)
    {
        _lock.unlock();
        return ret;
    }
    ret.cmd_id = _log.size();
    ret.term = _cur_term;
    ret.is_leader = true;

    log_entry log;
    log._op = op.get_cmd();
    log._term = _cur_term;
    _log.push_back(log);
    _lock.unlock();

    return ret;
}

vector<log_entry> Raft::string2log_entry(string s)
{
    vector<log_entry> ret;
    istringstream is(s);
    string tmp;
    while(getline(is, tmp, ';'))
    {
        log_entry t;
        int term = atoi(tmp.substr(0, tmp.find(' ')-1).c_str());
        string op = tmp.substr(tmp.find(' '));
        t._op = op;
        t._term = term;
        ret.push_back(t);
    }
    return ret;
}

pair<int, bool> Raft::get_state()
{
    pair<int, bool> ret;
    ret.first = _cur_term;
    ret.second = (_state == LEADER);
    return ret;
}

int Raft::get_duration(timeval t)
{
    timeval now;
    gettimeofday(&now, NULL);
    return (now.tv_sec - t.tv_sec) * 1000000 + (now.tv_usec - t.tv_usec);
}

void Raft::Make(vector<peer_node> peers, int id)
{
    this->peers = peers;
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
    //下一个需要同步的为1，从1开始
    _next_index.resize(peers.size(), 1);
    //已经同步的为0，这样就能区分开第一条和第0条
    _match_index.resize(peers.size(), 0);

    read_from_disk();

    pthread_t listen_1, listen_2, listen_3;
    pthread_create(&listen_1, NULL, listen4vote, this);
    pthread_detach(listen_1);
    pthread_create(&listen_2, NULL, listen4append, this);
    pthread_detach(listen_2);
    pthread_create(&listen_3, NULL, log_apply_loop, this);
    pthread_detach(listen_3);
}

void* Raft::listen4append(void* arg)
{
    Raft* raft = (Raft*) arg;
    buttonrpc server;
    server.as_server(raft->peers[raft->_peer_id]._port.first);
    server.bind("req_append", &Raft::recv_append, raft);

    pthread_t tid;
    pthread_create(&tid, NULL, log_process_loop, raft);
    pthread_detach(tid);

    server.run();
    std::cout<<"listen4append exit!"<<std::endl;
}

void* Raft::log_process_loop(void* arg)
{
    Raft* raft = (Raft*)arg;
    while(!raft->is_dead())
    {
        usleep(1000);
        raft->_lock.lock();
        if(raft->_state != LEADER)
        {
            raft->_lock.unlock();
            continue;
        }
        
        int since = raft->get_duration(raft->_last_broadcast_time);
        //没到时间
        if(since < HEARTBEAT_INTERVAL)
        {
            raft->_lock.unlock();
            continue;
        }

        //到时间了
        gettimeofday(&raft->_last_broadcast_time, NULL);
        raft->_lock.unlock();
        //req_append req;
        //req.term = raft->_cur_term;
        //req.leader_id = raft->_peer_id;
        //req.last_log_idx = raft->_log.size();
        pthread_t tids[raft->peers.size()-1];
        int idx = 0;
        for(auto& svr: raft->peers)
        {
            if(svr._id == raft->_peer_id)
                continue;
            pthread_create(&tids[idx], NULL, want2append, raft);
            pthread_detach(tids[idx++]);
        }
    }
}

void* Raft::want2append(void* arg)
{
    Raft *raft = (Raft*)arg;
    buttonrpc client;
    req_append req;
    raft->_lock.lock();

    //求当前要发送的peer_id
    if(raft->_cur_peer_id == raft->_peer_id)
    {
        raft->_cur_peer_id++;
    }
    int now_idx = raft->_cur_peer_id++;
    client.as_client("127.0.0.1", raft->peers[now_idx]._port.second);

    req.term = raft->_cur_term;
    req.leader_id = raft->_peer_id;
    req.last_log_idx = raft->_next_index[now_idx] - 1;
    req.leader_commit = raft->_commit_id;
    //发送commit过到最新的日志
    for(int i=req.last_log_idx; i<raft->_log.size(); i++)
    {
        req.send_logs += to_string(raft->_log[i]._term) + ' ' + raft->_log[i]._op + ';';
    }

    if(req.last_log_idx == 0)
    {
        req.last_log_term = 0;
        if(raft->_log.size() != 0)
        {
            req.last_log_term = raft->_log[0]._term;
        }
    }
    else
    {
        //last log term 一般存的是从节点上一次同步到的日志的最新term
        req.last_log_term = raft->_log[req.last_log_idx-1]._term;
    }

    printf("[%d]->[%d]: req append. last log idx: %d, last log term: %d", raft->_peer_id, now_idx, req.last_log_idx, req.last_log_term);
    raft->_lock.unlock();

    resp_append resp = client.call<resp_append>("want2append", req).val();

    raft->_lock.lock();

    //只要碰到对端term比自己大，直接变成follower
    if(resp.term > raft->_cur_term)
    {
        raft->_state = FOLLOWER;
        raft->_cur_term = resp.term;
        raft->_voted_for = -1;
        raft->write2disk();
        raft->_lock.unlock();
        return NULL;
    }

    if(resp.succ)
    {
        //_next_index： 下次该从哪里开始同步日志
        //_match_index:  已经确认同步的日志
        raft->_next_index[now_idx] += raft->string2log_entry(req.send_logs).size();
        raft->_match_index[now_idx] = raft->_next_index[now_idx]-1;

        //超过一半节点都同步了的日志，才能被leader标记为commited
        vector<int> tmp = raft->_match_index;
        sort(tmp.begin(), tmp.end(), std::less<int>());
        int majority_match_idx = tmp[tmp.size()/2];
        //这里为啥是majority_match_idx -1, 因为tmp，也就是_match_index存储的是真实idx+1
        if(majority_match_idx > raft->_commit_id && raft->_log[majority_match_idx-1]._term == raft->_cur_term)
        {
            raft->_commit_id = majority_match_idx;
        }
    }
    
    if(!resp.succ)
    {
        //冲突导致失败 term == -1的情况是被同步节点日志长度过短，无法判断是缺失还是冲突，需要减小_next_index来一步步判断
        if(resp.log_conflict_term != -1){
            // term ！= 1情况，说明确实存在有idx相同term却不同的entry，被同步端所有属于这个term的entry都需要被覆盖
            //所以再往前调整_next_Index
            int log_conflict_idx = -1;
            for(int i=0; i<raft->_log.size(); i++)
            {
                //找到冲突域term第一条log
                if(raft->_log[i]._term == resp.log_conflict_term)
                {
                    log_conflict_idx = i;
                    break;
                }
            }
            //leader节点也有这个term
            if(log_conflict_idx != -1)
            {
                raft->_next_index[now_idx] = log_conflict_idx + 1;
            }
            //找不到，直接从头开始同步
            else
            {
                //这里为啥是conflict term ?
                //raft->_next_index[now_idx] = resp.log_conflict_term;
                raft->_next_index[now_idx] = 1;
            }
        }
        else
        {
            raft->_next_index[now_idx] = resp.log_conflict_idx + 1;
        }
    }
    raft->write2disk();
    raft->_lock.unlock();
    return NULL;
}

resp_append Raft::recv_append(req_append req)
{
    vector<log_entry> logs = string2log_entry(req.send_logs);
    resp_append resp;
    _lock.lock();
    resp.term = _cur_term;
    resp.succ = false;
    resp.log_conflict_idx = -1;
    resp.log_conflict_term = -1;

    if(req.term < _cur_term)
    {
        _lock.unlock();
        return resp;
    }

    if(req.term > _cur_term)
    {
        _cur_term = req.term;
        _voted_for = -1;
        _state = FOLLOWER;
        write2disk();
    }

    gettimeofday(&_last_wake_time, NULL);

    int log_size = 0;
    //如果本身节点一条日志都没有，那就不存在冲突现象，直接更新
    if(_log.size() == 0)
    {
        for(auto &l:logs)
        {
            _log.push_back(l);
        }
        write2disk();
        log_size = _log.size();
        if(_commit_id < req.leader_commit)
        {
            //这里为什么是leader commit和log size较小值
            _commit_id = min(req.leader_commit, log_size);
        }
        _lock.unlock();
        resp.succ = true;
        return resp;
    }

    //本节点在leader上的checkpoint比当前节点idx还长，说明checkpoint前还有数据未同步
    if(_log.size() < req.last_log_idx)
    {
        resp.log_conflict_idx = _log.size();
        resp.succ = false;
        _lock.unlock();
        return resp;
    }

    //出现冲突，本届点的last log idx已经被前任leader更新过
    if(req.last_log_idx > 0 && _log[req.last_log_idx-1]._term != req.term)
    {
        //找到属于这个冲突term的第一个idx， 让leader下次把这一段日志捎带上
        resp.log_conflict_term = _log[req.last_log_idx-1]._term;
        for(int i=1; i<=req.last_log_idx; i++)
        {
            if(_log[i-1]._term == resp.log_conflict_term)
            {
                resp.log_conflict_idx = i;
                break;
            }
        }
        _lock.unlock();
        resp.succ = false;
        return resp;
    }

    log_size = _log.size();
    for(int i=req.last_log_idx; i<log_size; i++)
    {
        //这里可能会出现可删数据吗？
        _log.pop_back();
    }
    //先删除后插入，保证回复succ的节点日志列表长度和leader一致
    for(auto& it:logs)
    {
        _log.push_back(it);
    }
    write2disk();
    if(_commit_id < req.leader_commit)
    {
        _commit_id = min(req.leader_commit, (int)_log.size());
    }
    for(auto&it : _log)
    {
        std::cout<<it._term<<std::endl;
    }
    std::cout<<"append succ"<<std::endl;
    _lock.unlock();
    resp.succ = true;
    return resp;
}

void Raft::suicide()
{
    dead = 1;
}

bool Raft::check_log_up2date(int term, int index)
{
    _lock.lock();
    if(_log.size() == 0)
    {
        _lock.unlock();
        return true;
    }
    if(term > _log.back()._term)
    {
        _lock.unlock();
        return true;
    }
    if(term == _log.back()._term && index >= _log.size())
    {
        _lock.unlock();
        return true;
    }
    _lock.unlock();
    return false;
}

resp_vote Raft::recv_vote(req_vote req)
{
    resp_vote resp;
    resp.vote_grant = false;
    _lock.lock();
    resp.term = _cur_term;

    // 发来的请求已经过时，是之前任期的选票
    if(_cur_term > req.term)
    {
        _lock.unlock();
        return resp;
    }

    //当前节点本身出现了消息滞后，直接成为支持者，同时更新任期数据
    if(_cur_term < req.term)
    {
        _state = FOLLOWER;
        _cur_term = req.term;
        _voted_for = -1;
    }

    // 当前还未给投过票 ｜｜ 之前给candidate投的票丢失了，对端未收到
    if(_voted_for == -1 || _voted_for == req.candidate_id)
    {
        _lock.unlock();
        //请求被投票的选举人一定要比我新，我才给他投票，否则他的同步有问题不投票
        bool ret = check_log_up2date(req.last_log_term, req.last_log_idx);
        if(!ret)return resp;
        
        _lock.lock();
        _voted_for = req.candidate_id;
        resp.vote_grant = true;
        printf("[%d] vote for candidate: [%d]\n", _peer_id, req.candidate_id);
        gettimeofday(&_last_wake_time, NULL);
    }
    write2disk();
    _lock.unlock();
    return resp;
}

// leader执行，执行peers.size()-1次
void* Raft::want2vote(void* arg)
{
    Raft *raft = (Raft*)arg;
    buttonrpc client;
    req_vote req;
    raft->_lock.lock();
    req.candidate_id = raft->_peer_id;
    req.term = raft->_cur_term;
    req.last_log_idx = raft->_log.size();
    req.last_log_term = raft->_log.size() != 0 ? raft->_log.back()._term : 0;

    if(raft->_cur_peer_id == raft->_peer_id)raft->_cur_peer_id++;

    client.as_client("127.0.0.1", raft->peers[raft->_cur_peer_id++]._port.first);
    //if(raft->_cur_peer_id == raft->peers.size() || )
    raft->_lock.unlock();

    resp_vote resp = client.call<resp_vote>("recv_vote", req).val();

    raft->_lock.lock();
    raft->finish_votes++;
    raft->_cond.signal();
    if(resp.term > raft->_cur_term)
    {
        raft->_state = FOLLOWER;
        raft->_cur_term = resp.term;
        raft->_voted_for = -1;
        //这里是该读盘还是写盘？
        raft->write2disk();
        raft->_lock.unlock();
        return NULL;
    }
    if(resp.vote_grant)
    {
        raft->recv_votes++;
    }
    raft->_lock.unlock();
}

//不断检查是否到超时时间，一旦到达超时时间就发起选举（向其他节点发送req_vote)
void* Raft::election_loop(void* arg)
{
    Raft* raft = (Raft*)arg;
    bool reset = false;
    while(!raft->is_dead())
    {
        int timeout = rand()%200000 + 200000;
        while(1){
            usleep(1000);
            raft->_lock.lock();

            int since = raft->get_duration(raft->_last_wake_time);
            if(raft->_state == FOLLOWER && since > timeout)
            {
                raft->_state = CANDIDATE;
            }

            if(raft->_state == CANDIDATE && since > timeout)
            {
                std::cout<<raft->_peer_id<<" attempt to elect in term: "<<raft->_cur_term<<std::endl;
                gettimeofday(&raft->_last_wake_time, NULL);
                reset = true;
                raft->_cur_term++;
                raft->_voted_for = raft->_peer_id;
                //一旦修改就要存盘
                raft->write2disk();

                raft->recv_votes = 1;
                raft->finish_votes = 1;
                raft->_cur_peer_id = 0;
                pthread_t tids[raft->peers.size()-1];
                int t = 0;
                for(int i=0; i<raft->peers.size(); i++)
                {
                    if(raft->peers[i]._id == raft->_peer_id)
                    {
                        continue;
                    }
                    //发送选举请求
                    pthread_create(&tids[t], NULL, want2vote, raft);
                    pthread_detach(tids[t]);
                    t++;
                }
                //没有收到所有的回复，活着当前选票还不够
                while(raft->recv_votes <= raft->peers.size() / 2 || raft->finish_votes < raft->peers.size())
                {
                    raft->_cond.wait(&raft->_lock);
                }
                if(raft->_state != CANDIDATE)
                {
                    raft->_lock.unlock();
                    continue;
                }
                //获得了超过半数的选票
                if(raft->recv_votes > raft->peers.size() / 2)
                {
                    raft->_state = LEADER;
                    //设置日志同步参数
                    for(int i=0; i<raft->peers.size(); i++)
                    {
                        //新上任leader先给从节点更新上任后到来的日志，如果不匹配
                        raft->_next_index[i] = raft->_log.size() + 1;
                        raft->_match_index[i] = 0;
                    }
                    std::cout<<raft->_peer_id<<" become a leader in term: "<<raft->_cur_term<<std::endl;
                    raft->set_broadcast_time();
                }
            }
            raft->_lock.unlock();
            if(reset)
            {
                reset = false;
                //break出去重新设置超时时间
                break;
            }
        }
    }
}

void Raft::set_broadcast_time()
{
    gettimeofday(&_last_broadcast_time, NULL);
    std::cout<<"last broadcast time: "<<_last_broadcast_time.tv_sec<<":"<<_last_broadcast_time.tv_usec<<std::endl;
    if(_last_broadcast_time.tv_usec >= 200000)
    {
        _last_broadcast_time.tv_usec -= 200000;
    }
    else
    {
        _last_broadcast_time.tv_sec -= 1;
        _last_broadcast_time.tv_usec -= 800000;
    }
}

void* Raft::listen4vote(void* arg)
{
    Raft* raft = (Raft*)arg;
    buttonrpc server;
    server.as_server(raft->peers[raft->_peer_id]._port.second);
    server.bind("req_vote", &Raft::recv_vote, raft);

    pthread_t wait_tid;
    pthread_create(&wait_tid, NULL, election_loop, raft);
    pthread_detach(wait_tid);

    server.run();
    std::cout<<"listen4vote exit!"<<std::endl;
}

void Raft::read_from_disk()
{
    bool ret = this->deserialize();
    if(!ret) return;
    this->_cur_term = this->persister.now_term;
    this->_voted_for = this->persister.vote_for;

    for(auto& log: this->persister.logs)
    {
        _log.push_back(log);
    }
    printf("[%d]'s term: %d, vote for: %d, log.size() = %d", this->_peer_id, this->_cur_term, this->_voted_for, this->_log.size());
}

void Raft::write2disk()
{
    //这里需要加锁吗
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
    int fd;
    if(access(filename.c_str(), F_OK) == -1)
        fd = open(filename.c_str(), O_RDWR | O_CREAT, 0664);
    else
        fd = open(filename.c_str(), O_RDWR | O_TRUNC, 0664);
    if(fd == -1)
    {
        perror("open");
        exit(-1);
    }
    int len = write(fd, str.c_str(), str.length());
    assert(len == str.length());
}

bool Raft::deserialize(){
    string path = "persister-" + to_string(_peer_id);
    if(access(path.c_str(), F_OK) == -1){
        perror("open");
        return false;
    }
    int fd = open(path.c_str(), O_RDONLY);
    if(fd == -1)
    {
        perror("open");
        return false;
    }
    int length = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    char* buf = new char[length];
    memset(buf, 0, sizeof(buf));
    int len = read(fd, buf, length);
    assert(len == length);

    istringstream ss(buf);
    vector<string> tmp_list;
    string tmp;
    while(getline(ss, tmp, ';'))
    {
        tmp_list.push_back(tmp);
    }
    assert(tmp_list.size() == 3);
    this->persister.now_term = atoi(tmp_list[0].c_str());
    this->persister.vote_for = atoi(tmp_list[1].c_str());

    vector<log_entry> log_tmp;
    istringstream logs(tmp_list[2]);
    string log;
    while(getline(logs, log, '.'))
    {
        auto it = log.find(',');
        log_tmp.push_back(log_entry(log.substr(0, it), atoi(log.substr(it+1).c_str())));
    }
    this->persister.logs = log_tmp;
}

void* Raft::log_apply_loop(void * arg)
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
    std::cout<<peercnt<<std::endl;
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
    std::cout<<"Test Over!"<<std::endl;
    while(1);
}