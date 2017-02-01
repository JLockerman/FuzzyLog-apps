#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include "worker.h"

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;

void run_YCSB(uint32_t record_count, uint32_t operation_count, uint32_t num_workers, uint32_t partition_count) {
        uint32_t i, operation_per_worker, remainder;
        HashMap *map;
        Table *table;
        workload_generator *workload_gen;
        Txn** txns;
        vector<Worker*> workers;


        // Fuzzymap
        map = new HashMap(partition_count);

        // Populate table 
        table = new Table(record_count);
   //   cout << "Populating table with " << record_count << " records ..." << endl;
   //   table->populate(map);
   //   cout << "Done." << endl;
               
        // Generate update workloads
        // distribution : uniform
        workload_gen = new workload_generator(table, operation_count);
        txns = workload_gen->Gen();
        
        // Assign workloads to worker threads
        operation_per_worker = operation_count / num_workers; 
        remainder = operation_count % num_workers;

        workers.reserve(num_workers);
        for (i = 0; i < num_workers; ++i) {
                if (i == num_workers - 1)
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker + remainder));
                else
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker));
        }

        auto start = get_time::now(); //use auto keyword to minimize typing strokes :)

        // Run workers
        for (i = 0; i < num_workers; ++i)
                workers[i]->run();
 
        for (i = 0; i < num_workers; ++i)
                pthread_join(*workers[i]->get_pthread_id(), NULL);

        auto end = get_time::now();
        chrono::duration<double> diff = end - start;
        cout << num_workers << " " << partition_count << " " << diff.count() << " " << operation_count / diff.count() << endl;
        //cout << record_count << " " << operation_count << " " << num_workers << " " << partition_count << " " << diff.count() << endl;
}

int main(int argc, char** argv) {
        // FIXME: only necessary for local test
	start_fuzzy_log_server_thread("0.0.0.0:9990");
        
        if (argc != 5) {
                cout << "Usage: ./build/hashmap record_count operation_count num_workers partition_count" << endl;
                return 0;
        }

        uint32_t record_count, operation_count, num_workers, partition_count;
        record_count = atoi(argv[1]);
        operation_count = atoi(argv[2]);
        num_workers = atoi(argv[3]);
        partition_count = atoi(argv[4]);
        // run YCSB
        run_YCSB(record_count, operation_count, num_workers, partition_count); 

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