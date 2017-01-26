#include "hashmap.h"
#include <iostream>

extern "C" {
        #include "fuzzy_log.h"
}

static const char *server_ip = "127.0.0.1:9990";
// FIXME: for experiment, use the below server
//static const char *server_ip = "52.15.156.76:9990";

static char out[DELOS_MAX_DATA_SIZE];

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
        DAGHandle* dag = new_dag_handle_for_single_server(server_ip, &color);

        // snapshot, get_next
        snapshot(dag);
        while ((get_next(dag, out, &size, &color), 1)) {
                if (color.numcolors == 0) break;
                data = *(uint64_t *)out; 
                if ((uint32_t)(data >> 32) != key) continue;
                val = (uint32_t)(data & 0xFFFFFFFF); 
        }

        // close
        close_dag_handle(dag);

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
        DAGHandle* dag = new_dag_handle_for_single_server(server_ip, &color);

        // append
        uint64_t data = ((uint64_t)key << 32) | value;
        
        append(dag, (char *)&data, sizeof(data), &color, NULL);

        // close
        close_dag_handle(dag);
}


void HashMap::remove(uint32_t key) {
        // make color
        struct colors color;
        color.numcolors = 1;
        color.mycolors = new ColorID[0]; 
        color.mycolors[0] = key % PARTITION_COUNT;
        
        // connect and get handler
        DAGHandle* dag = new_dag_handle_for_single_server(server_ip, &color);

        // append
        uint64_t data = (uint64_t)key << 32;
        append(dag, (char *)&data, sizeof(data), &color, NULL);

        // close
        close_dag_handle(dag);

        // remove cache
        cache.erase(key);
}
