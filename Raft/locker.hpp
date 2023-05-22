#ifndef LOCKER
#define LOCKER
#include <iostream>
#include <pthread.h>

using namespace std;

class locker{
    public:
        locker(){
            if(pthread_mutex_init(&_m, NULL) != 0)
            {
                throw std::exception();
            }
        }
        ~locker(){
            pthread_mutex_destroy(&_m);
        }
        void lock()
        {
            pthread_mutex_lock(&_m) == 0;
        }
        void unlock()
        {
            pthread_mutex_unlock(&_m) == 0;
        }
        pthread_mutex_t* getlock()
        {
            return &_m;
        }
    private:
        pthread_mutex_t _m;
};

class cond {
        pthread_cond_t _cond;
    public:
        cond(){
            if(pthread_cond_init(&_cond, NULL) != 0)
            {
                throw std::exception();
            }
        }
        ~cond(){
            pthread_cond_destroy(&_cond);
        }
        void wait(locker* mutex){
            pthread_cond_wait(&_cond, mutex->getlock());
        }
        void time_wait(locker* mutex, const struct timespec* t)
        {
            pthread_cond_timedwait(&_cond, mutex->getlock(), t);
        }
        void signal()
        {
            pthread_cond_signal(&_cond);
        }
        void broadcast()
        {
            pthread_cond_broadcast(&_cond);
        }
};
#endif