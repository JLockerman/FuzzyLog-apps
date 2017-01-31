#include "hashmap.h"
#include <iostream>

//static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap(uint32_t partition_count) {
        this->partition_count = partition_count;

        fuzzylog_clients = (DAGHandle**)(malloc(sizeof(DAGHandle*) * partition_count));
        for (size_t i = 0; i < partition_count; ++i) {
                fuzzylog_clients[i] = NULL;
        }

        // init locks
        locks.reserve(partition_count);
        for (size_t i = 0; i < partition_count; ++ i) {
                locks.push_back(new std::mutex);
        }
}

HashMap::~HashMap() {
        for (size_t i = 0; i < partition_count; ++i) {
                close_dag_handle(fuzzylog_clients[i]);
        }
        delete[] fuzzylog_clients;
}

uint32_t HashMap::get(uint32_t key) {
        struct colors color;
        uint64_t data;
        uint32_t val;
        size_t size = 0;
        uint32_t partition_num;

        partition_num = key % partition_count;
        
        // acquire lock
        locks[partition_num]->lock();

        // make color
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = partition_num;

        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[partition_num];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[partition_num] = dag;
        }

        // snapshot, get_next
        snapshot(dag);
        while ((get_next(dag, out, &size, &color), 1)) {
                if (color.numcolors == 0) break;
                data = *(uint64_t *)out; 
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // check if no entry
        if (val == 0) {
                // FIXME: instead, throw exception?
                locks[partition_num]->unlock();
                return 0;
        }

        // cache the value
        cache[key] = val;        

        // release lock 
        locks[partition_num]->unlock();

        return val;
}


void HashMap::put(uint32_t key, uint32_t value) {
        struct colors color;
        uint32_t partition_num;

        partition_num = key % partition_count;

        // acquire lock
        locks[partition_num]->lock();

        // make color
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = partition_num;
        
        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[partition_num];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[partition_num] = dag;
        }

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        
        append(dag, (char *)&data, sizeof(data), &color, NULL);

        // release lock
        locks[partition_num]->unlock();
}


void HashMap::remove(uint32_t key) {
        struct colors color;
        uint32_t partition_num;

        partition_num = key % partition_count;

        // acquire lock
        locks[partition_num]->lock();

        // make color
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = partition_num;
        
        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[partition_num];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[partition_num] = dag;
        }

        // append
        uint64_t data = (uint64_t)key << 32;
        append(dag, (char *)&data, sizeof(data), &color, NULL);

        // remove cache
        cache.erase(key);

        // release lock
        locks[partition_num]->unlock();
}
