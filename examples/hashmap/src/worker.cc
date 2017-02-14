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

void* Worker::bootstrap(void *arg) {
        uint32_t i;
        Worker *worker = (Worker*)arg;

        if (ASYNC_WRITE) {
                for (i = 0; i < worker->m_num_txns; ++i) {
                        worker->m_txns[i]->AsyncRun(worker->m_map);
                }
                worker->m_map->wait_all();

        } else {
                for (i = 0; i < worker->m_num_txns; ++i) {
                        worker->m_txns[i]->Run(worker->m_map);
                }
        }
        
        return NULL;
}

pthread_t* Worker::get_pthread_id() {
        return &m_thread;
}
