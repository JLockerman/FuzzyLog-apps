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
        HashMap *map = worker->m_map;

        if (ASYNC_WRITE) {
                // Send requests
                for (i = 0; i < WINDOW_SIZE; i++) {
                        worker->m_txns[i]->AsyncRun(map);
                }
                for (i = WINDOW_SIZE; i < worker->m_num_txns; i++) {
                        map->wait_for_any_put();
                        worker->m_txns[i]->AsyncRun(map);
                }
                map->wait_for_all();
                // Write latency output 
                worker->m_map->write_output_for_latency("scripts/latency_async.txt");
        } else {
                for (i = 0; i < worker->m_num_txns; ++i) {
                        worker->m_txns[i]->Run(worker->m_map);
                }
                // Write latency output 
                worker->m_map->write_output_for_latency("scripts/latency_sync.txt");
        }

        
        return NULL;
}

pthread_t* Worker::get_pthread_id() {
        return &m_thread;
}
