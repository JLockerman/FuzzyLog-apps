#include "hashmap.h"
#include <iostream>

static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
//static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

HashMap::HashMap() {
        fuzzylog_clients = (DAGHandle**)(malloc(sizeof(DAGHandle*) * PARTITION_COUNT));
        for (size_t i = 0; i < PARTITION_COUNT; ++i) {
                fuzzylog_clients[i] = NULL;
        }
}

HashMap::~HashMap() {
        for (size_t i = 0; i < PARTITION_COUNT; ++i) {
                close_dag_handle(fuzzylog_clients[i]);
        }
        delete[] fuzzylog_clients;
}

uint32_t HashMap::get(uint32_t key) {
        struct colors color;
        uint64_t data;
        uint32_t val;
        size_t size = 0;

        // make color
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = key % PARTITION_COUNT;

        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[key % PARTITION_COUNT];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[key % PARTITION_COUNT] = dag;
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
                return 0;
        }

        // cache the value
        cache[key] = val;        

        return val;
}


void HashMap::put(uint32_t key, uint32_t value) {
        // make color
        struct colors color;
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = key % PARTITION_COUNT;
        
        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[key % PARTITION_COUNT];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[key % PARTITION_COUNT] = dag;
        }

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        
        append(dag, (char *)&data, sizeof(data), &color, NULL);
}


void HashMap::remove(uint32_t key) {
        // make color
        struct colors color;
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = key % PARTITION_COUNT;
        
        // connect and get handler
        DAGHandle* dag = fuzzylog_clients[key % PARTITION_COUNT];
        if (dag == NULL) {
                dag = new_dag_handle_for_single_server(server_ip, &color);
                fuzzylog_clients[key % PARTITION_COUNT] = dag;
        }

        // append
        uint64_t data = (uint64_t)key << 32;
        append(dag, (char *)&data, sizeof(data), &color, NULL);

        // remove cache
        cache.erase(key);
}
