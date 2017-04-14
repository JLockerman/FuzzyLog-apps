#pragma once

#include <pthread.h>
#include <capmap_workload.h>
#include <tester.h>
#include <iostream>

// CAPMapTester class which should be unaware of AtomicMap 
class CAPMapTester : public Tester {
public:
        CAPMap*                         m_capmap;  // FIXME: hacky way for calling fuzzylog api...
       
public:
        CAPMapTester(Context* context, CAPMap* map, std::atomic<bool>* flag, Txn** txns, uint32_t num_txns, bool async, uint32_t window_size, uint32_t expt_duration, uint32_t txn_rate): Tester(context, map, flag, txns, num_txns, async, window_size, expt_duration, txn_rate), m_capmap(map) {}
        ~CAPMapTester();
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void ExecuteProtocol2(std::string& role); 
};
