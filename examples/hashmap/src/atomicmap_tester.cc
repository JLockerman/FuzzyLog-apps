#include <atomicmap_tester.h>
#include <cstring>
#include <cassert>

AtomicMapTester::~AtomicMapTester() {
        for (size_t i = 0; i < m_num_txns; i++) {
                ycsb_insert* txn = dynamic_cast<ycsb_insert*>(m_txns[i]);
                delete txn;
        }
        delete m_txns;
}

void AtomicMapTester::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void AtomicMapTester::join() {
        pthread_join(m_thread, NULL);
}

void* AtomicMapTester::bootstrap(void *arg) {
        AtomicMapTester *worker = static_cast<AtomicMapTester*>(arg);
        worker->Execute();
        return NULL;
}

void AtomicMapTester::Execute() {
        uint32_t i;
        uint32_t num_pending = 0; 
        reset_throttler();

        if (m_async) {
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

                        m_txns[i]->AsyncRun();
                        if (m_txns[i]->op_type() == Txn::optype::PUT) {
                                m_num_issued++;
                                num_pending += 1;
                        }
                }
                m_map->wait_for_all_puts();
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;

        } else {
                // Repeat request while flag = true
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        if (!is_duration_based_run() && is_all_executed()) break;

                        auto start_time = std::chrono::system_clock::now();
                        m_txns[i]->Run();
                        add_put_latency(start_time);
                }
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
        }
}
