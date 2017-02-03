#include "hashmap.h"
#include <iostream>

//static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap(struct colors* color) {
        this->color = color;
        fzlog_client = new_dag_handle_for_single_server(server_ip, color);
}

HashMap::~HashMap() {
        close_dag_handle(fzlog_client);
}

uint32_t HashMap::get(uint32_t key) {
        uint64_t data;
        uint32_t val;
        size_t size = 0;

        // acquire lock
        fzlog_lock.lock();

        // snapshot, get_next
        snapshot(fzlog_client);
        while ((get_next(fzlog_client, out, &size, color), 1)) {
                if (color->numcolors == 0) break;
                data = *(uint64_t *)out; 
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // check if no entry
        if (val == 0) {
                // FIXME: instead, throw exception?
                fzlog_lock.unlock();
                return 0;
        }

        // cache the value
        cache[key] = val;        

        // release lock 
        fzlog_lock.unlock();

        return val;
}


void HashMap::put(uint32_t key, uint32_t value) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        append(fzlog_client, (char *)&data, sizeof(data), color, NULL);

        // release lock
        fzlog_lock.unlock();
}


void HashMap::remove(uint32_t key) {
        // acquire lock
        fzlog_lock.lock();

        // append
        uint64_t data = (uint64_t)key << 32;
        append(fzlog_client, (char *)&data, sizeof(data), color, NULL);

        // remove cache
        cache.erase(key);

        // release lock
        fzlog_lock.unlock();
}
