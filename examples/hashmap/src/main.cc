#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include "worker.h"

extern "C" {
        #include "fuzzy_log.h"
}

#define NUM_WORKERS 8

using namespace std;

void run_YCSB() {
        uint32_t i, record_count, operation_count, operation_per_worker, remainder;
        HashMap *map;
        Table *table;
        workload_generator *workload_gen;
        Txn** txns;
        vector<Worker*> workers;

        // Parameter
        record_count = 10000;
        operation_count = 10000;

        // Fuzzymap
        map = new HashMap;

        // Generate table 
        table = new Table(record_count);
        table->populate(map);
               
        // TODO: Generate update workloads
        // distribution : uniform
        workload_gen = new workload_generator(table, operation_count);
        txns = workload_gen->Gen();
        
        // TODO: Worker threads
        // TODO: Assign workloads to worker threads
        operation_per_worker = operation_count / NUM_WORKERS; 
        remainder = operation_count % NUM_WORKERS;

        workers.reserve(NUM_WORKERS);
        for (i = 0; i < NUM_WORKERS; ++i) {
                if (i == NUM_WORKERS - 1)
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker + remainder));
                else
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker));
        }

        for (i = 0; i < NUM_WORKERS; ++i)
                workers[i]->run();
 
}

int main() {
        // FIXME: only necessary for local test
	start_fuzzy_log_server_thread("0.0.0.0:9990");
        
        // Parse workload file from YCSB
        run_YCSB();        

//      HashMap map;
//      map.put(10, 15);
//      map.put(10, 16);
//      map.put(10, 12);

//      map.put(100, 5);
//      map.remove(100);
//      map.put(100, 7);

//      
//      uint32_t val10 = map.get(10);
//      std::cout << "Output for key 10 : " << val10 << std::endl;

//      uint32_t val100 = map.get(100);
//      std::cout << "Output for key 100: " << val100 << std::endl;

        return 0;
}
