#include <worker.h>
#include <cstring>
#include <cassert>

Worker::~Worker() {
        for (size_t i = 0; i < m_num_txns; i++) {
                ycsb_insert* txn = (ycsb_insert*)m_txns[i];
                delete txn;
        }
        delete m_txns;
}

void Worker::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void Worker::join() {
        pthread_join(m_thread, NULL);
}

void* Worker::bootstrap(void *arg) {
        Worker *worker = (Worker*)arg;
        worker->Execute();
        return NULL;
}

pthread_t* Worker::get_pthread_id() {
        return &m_thread;
}

void Worker::Execute() {
        uint32_t i;
        bool windowing_started;
      
        if (m_async) {
                // Repeat request while flag = true
                windowing_started = false;
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        if (m_num_txns <= m_window_size) {
                                m_txns[i]->AsyncRun();
                        } else {
                                // Windowing
                                if (i < m_window_size && !windowing_started) {
                                        m_txns[i]->AsyncRun();
                                } else { 
                                        windowing_started = true;
                                        // Wait
                                        new_write_id nwid = m_txns[0]->wait_for_any_op();
                                        if (nwid == NEW_WRITE_ID_NIL) {
                                                std::cout << "no pending writes. terminate" << std::endl; 
                                                break;
                                        }
                                        // Append again
                                        m_txns[i]->AsyncRun();
                                }
                        }
                }
                m_txns[0]->wait_for_all_ops();
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
        } else {
                // Repeat request while flag = true
                for (i = 0; *m_flag; i++, i = i % m_num_txns) {
                        m_txns[i]->Run();
                }
                m_context->set_finished();

                std::cout << "total executed: " << m_context->get_num_executed() << std::endl;
        }
}

uint64_t Worker::get_num_executed() {
        return m_context->get_num_executed();
}
