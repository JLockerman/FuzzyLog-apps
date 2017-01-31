#include <worker.h>
#include <cstring>

#define PRNG_BUFSZ 32
void Runnable::rand_init() {
        char *random_buf;

        m_rand_state = (struct random_data*)malloc(sizeof(struct random_data));
        memset(m_rand_state, 0x0, sizeof(struct random_data));
        random_buf = (char*)malloc(PRNG_BUFSZ);
        memset(random_buf, 0x0, PRNG_BUFSZ);
        initstate_r(random(), random_buf, PRNG_BUFSZ, m_rand_state);
}

Runnable::Runnable() {
        rand_init();
}

int Runnable::get_random() {
        int ret;
        random_r(m_rand_state, &ret);
        return ret;
}

void Worker::run() {
        pthread_create(&m_thread, NULL, bootstrap, this);
}

void* Worker::bootstrap(void *arg) {
        uint32_t i;
        Worker *worker = (Worker*)arg;

        // run txn
        for (i = 0; i < worker->num_txns; ++i) {
                worker->txns[i]->Run(worker->map);
        }
        return NULL;
}
