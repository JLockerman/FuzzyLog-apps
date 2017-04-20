#include <txmap_tester.h>
#include <cstring>
#include <cassert>
#include <thread>

TXMapTester::~TXMapTester() {
        for (size_t i = 0; i < m_num_txns; i++) {
                read_write_txn* txn = dynamic_cast<read_write_txn*>(m_txns[i]);
                delete txn;
        }
        delete m_txns;
}

void TXMapTester::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void TXMapTester::join() {
        pthread_join(m_thread, NULL);
}

void* TXMapTester::bootstrap(void *arg) {
        TXMapTester *worker = static_cast<TXMapTester*>(arg);
        worker->Execute();
        return NULL;
}

uint32_t TXMapTester::try_get_completed() {
        while (true) {
                write_id wid = m_map->try_wait_for_any_put();
                if (wid_equality{}(wid, WRITE_ID_NIL)) 
                        break; 
        }
        return 0;
}

void TXMapTester::Execute() {
        uint32_t i;
        TXMapContext *ctx = static_cast<TXMapContext*>(m_context);
        reset_throttler();

        assert(m_async);

        ctx->start_measure_time();
        for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                // try get completed
                try_get_completed();

                if (!is_duration_based_run() && is_all_executed()) break;
                                        
                // throttle
                if (is_throttled()) continue;

                // issue
                if (ctx->is_window_filled()) {
                        std::this_thread::sleep_for(std::chrono::nanoseconds(10));
                        continue;
                }
                m_txns[i]->Run();
                ctx->inc_num_executed();
                ctx->inc_num_pending_txns();
        }
        ctx->set_finished();
}

uint64_t TXMapTester::get_num_committed() {
        TXMapContext *ctx = static_cast<TXMapContext*>(m_context);
        return ctx->get_num_committed();
}
