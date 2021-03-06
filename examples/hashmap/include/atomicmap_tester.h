#pragma once

#include <atomicmap_workload.h>
#include <tester.h>
#include <iostream>

// AtomicMapTester class which should be unaware of AtomicMap 
class AtomicMapTester : public Tester {
public:
        AtomicMap*                      m_atomicmap;  // FIXME: hacky way for calling fuzzylog api...
        
public:
        AtomicMapTester(Context* context, AtomicMap* map, std::atomic<bool>* flag, Txn** txns, uint32_t num_txns, bool async, uint32_t window_size, uint32_t expt_duration, uint32_t txn_rate): Tester(context, map, flag, txns, num_txns, async, window_size, expt_duration, txn_rate), m_atomicmap(map) {}
        ~AtomicMapTester();
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void Execute(); 
};
