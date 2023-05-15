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
#endif