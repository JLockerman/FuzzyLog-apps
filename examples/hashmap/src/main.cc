#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include "worker.h"

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;


void write_output(uint32_t client_id, double result) {
        std::ofstream result_file; 
        result_file.open("result/" + std::to_string(client_id) + ".txt", std::ios::app | std::ios::out);
        result_file << result << "\n"; 
        result_file.close();        
}

void run_YCSB(vector<uint32_t> *colors, uint32_t single_operation_count, uint32_t multi_operation_count) {
        uint32_t i, total_operation_count, operation_per_worker, remainder;
        HashMap *map;
        workload_generator *workload_gen;
        Txn** txns;
        vector<Worker*> workers;
        uint32_t num_workers;
        num_workers = 1;        // FIXME

        // Fuzzymap
        map = new HashMap(colors);

        uint32_t range = 100000;
               
        // Generate update workloads
        // distribution : uniform
        workload_gen = new workload_generator(range, colors, single_operation_count, multi_operation_count);
        txns = workload_gen->Gen();
        
        // Assign workloads to worker threads
        total_operation_count = single_operation_count + multi_operation_count;
        operation_per_worker = total_operation_count / num_workers; 
        remainder = total_operation_count % num_workers;

        workers.reserve(num_workers);
        for (i = 0; i < num_workers; ++i) {
                if (i == num_workers - 1)
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker + remainder));
                else
                        workers.push_back(new Worker(map, txns + i * operation_per_worker, operation_per_worker));
        }

        auto start = get_time::now();

        // Run workers
        for (i = 0; i < num_workers; ++i)
                workers[i]->run();
 
        for (i = 0; i < num_workers; ++i)
                pthread_join(*workers[i]->get_pthread_id(), NULL);


        // Measure duration 
        auto end = get_time::now();
        chrono::duration<double> diff = end - start;

        // Write to output file
        write_output(colors->at(0), total_operation_count / diff.count());
        
        // Free
        for (i = 0; i < num_workers; ++i)
                delete workers[i];
        delete map;
        delete workload_gen;
}


int main(int argc, char** argv) {
        if (argc != 5) {
                cout << "Usage: ./build/hashmap <local_color> <remote_color> <single_append_op_count> <multi_append_op_count>" << endl;
                return 0;
        }

        vector<uint32_t> colors;
        uint32_t single_operation_count, multi_operation_count;

        colors.reserve(2);
        colors.push_back(atoi(argv[1]));
        colors.push_back(atoi(argv[2]));

        single_operation_count = atoi(argv[3]);
        multi_operation_count = atoi(argv[4]);

        run_YCSB(&colors, single_operation_count, multi_operation_count); 
        
        return 0;
}
