#include <worker.h>
#include <cstring>
#include <cassert>

void Worker::run() {
        int err;
        err = pthread_create(&m_thread, NULL, bootstrap, this);
        assert(err == 0);
}

void* Worker::bootstrap(void *arg) {
        uint32_t i, num_txns;
        Worker *worker = (Worker*)arg;
        num_txns = worker->num_txns;

        // run txn
        for (i = 0; i < num_txns; ++i) {
                worker->txns[i]->Run(worker->map);
        }
        return NULL;
}

pthread_t* Worker::get_pthread_id() {
        return &m_thread;
}
