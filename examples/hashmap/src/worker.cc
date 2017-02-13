#include <worker.h>
#include <cstring>
#include <cassert>

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
