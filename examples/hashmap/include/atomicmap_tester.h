#pragma once

#include <pthread.h>
#include <atomicmap_workload.h>
#include <tester.h>
#include <iostream>

// AtomicMapTester class which should be unaware of AtomicMap 
class AtomicMapTester : public Tester {
public:
        Context*                        m_context;
        AtomicMap*                      m_map;  // FIXME: hacky way for calling fuzzylog api...
        Txn**                           m_txns;
        uint32_t                        m_num_txns;
        bool                            m_async;
        uint32_t                        m_window_size;
        uint32_t                        m_expt_duration;
        std::atomic<bool>*              m_flag;
        // Number of executed operations
        std::atomic<uint64_t>           m_num_executed; 
        
public:
        AtomicMapTester(Context* context, AtomicMap* map, std::atomic<bool>* flag, Txn** txns, uint32_t num_txns, bool async, uint32_t window_size, uint32_t expt_duration): Tester() {
                this->m_context = context;
                this->m_map = map;
                this->m_flag = flag;
                this->m_txns = txns;
                this->m_num_txns = num_txns;
                this->m_async = async;
                this->m_window_size = window_size;
                this->m_expt_duration = expt_duration;
                this->m_num_executed = 0;
        }
        ~AtomicMapTester();
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        pthread_t* get_pthread_id();
        void Execute(); 
        uint64_t get_num_executed();
        uint32_t try_get_completed();
        bool is_duration_based_run();
        bool is_all_executed();
};
