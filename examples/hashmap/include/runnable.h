#pragma once

#include <pthread.h>

class Runnable {
protected:
        pthread_t                       m_thread;
public:
        Runnable() {}
        virtual ~Runnable() {}
        virtual void run() = 0;
        virtual void join() = 0;
};
