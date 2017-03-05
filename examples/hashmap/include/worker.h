#pragma once

#include <pthread.h>
#include <workload.h>
#include <iostream>

class Runnable {
protected:
        pthread_t                       m_thread;
public:
        Runnable() {}
        virtual ~Runnable() {}
        virtual void run() = 0;
        virtual void join() = 0;
};

// Worker class which should be unaware of HashMap 
class Worker : public Runnable {
public:
        Context*                        m_context;
        Txn**                           m_txns;
        uint32_t                        m_num_txns;
        bool                            m_async;
        uint32_t                        m_window_size;
        std::atomic<bool>*              m_flag;
        // Number of executed operations
        std::atomic<uint64_t>           m_num_executed; 
        
public:
        Worker(Context* context, std::atomic<bool>* flag, Txn** txns, uint32_t num_txns, bool async, uint32_t window_size): Runnable() {
                this->m_context = context;
                this->m_flag = flag;
                this->m_txns = txns;
                this->m_num_txns = num_txns;
                this->m_async = async;
                this->m_window_size = window_size;
                this->m_num_executed = 0;
        }
        ~Worker();
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        pthread_t* get_pthread_id();
        void Execute(); 
        uint64_t get_num_executed();
};
