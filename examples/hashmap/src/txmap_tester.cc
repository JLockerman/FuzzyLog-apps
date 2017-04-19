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

void TXMapTester::Execute() {
        uint32_t i;
        uint32_t num_pending = 0; 
        reset_throttler();

        assert(m_async);
        for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                // try get completed
                num_pending -= try_get_completed();
                if (!is_duration_based_run() && is_all_executed()) break;
                                        
                // throttle
                if (is_throttled()) continue;

                // issue
                if (num_pending == m_window_size) {
                        write_id wid = m_map->wait_for_any_put();
                        if (wid_equality{}(wid, WRITE_ID_NIL)) { 
                                std::cout << "no pending writes. terminate" << std::endl; 
                                break;
                        }
                        num_pending -= 1;
                        m_context->inc_num_executed();
                        if (!is_duration_based_run() && is_all_executed()) break;
                }
                m_txns[i]->Run();
                m_num_issued++;
                num_pending += 1;
        }
        while (num_pending > 0) {
                num_pending -= try_get_completed();
                std::this_thread::sleep_for(std::chrono::microseconds(1));
        } 
        m_context->set_finished();

        std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
}
