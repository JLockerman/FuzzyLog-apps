#pragma once

#include <txmap_workload.h>
#include <tester.h>
#include <iostream>

// TXMapTester class which should be unaware of TXMap 
class TXMapTester : public Tester {
public:
        TXMap*                      m_txmap;  // FIXME: hacky way for calling fuzzylog api...
        
public:
        TXMapTester(Context* context, TXMap* map, std::atomic<bool>* flag, Txn** txns, uint32_t num_txns, bool async, uint32_t window_size, uint32_t expt_duration, uint32_t txn_rate): Tester(context, map, flag, txns, num_txns, async, window_size, expt_duration, txn_rate), m_txmap(map) {}
        ~TXMapTester();
        virtual void run();
        virtual void join();
        static void* bootstrap(void *arg);
        void Execute(); 
};
