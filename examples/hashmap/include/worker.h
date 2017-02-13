#ifndef         WORKER_H_
#define         WORKER_H_

#include <pthread.h>
#include <workload.h>
#include <iostream>

class Runnable {
protected:
        pthread_t                       m_thread;
public:
        Runnable() {}
        virtual void run() = 0;
};

class Worker : public Runnable {
public:
        HashMap*                        m_map;
        Txn**                           m_txns;
        uint32_t                        m_num_txns;
public:
        Worker(HashMap *map, Txn** txns, uint32_t num_txns): Runnable() {
                this->m_map = map;
                this->m_txns = txns;
                this->m_num_txns = num_txns;
        }
        virtual void run();
        static void* bootstrap(void *arg);
        pthread_t* get_pthread_id();
};

#endif          // WORKER_H_
