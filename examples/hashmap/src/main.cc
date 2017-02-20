#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <worker.h>

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

void do_experiment(config cfg) {
        uint32_t total_op_count;
        HashMap *map;
        workload_generator *workload_gen;
        Txn** txns;
        Worker* worker;

        // Fuzzymap
        map = new HashMap(&cfg.log_addr);

        // Generate append workloads: uniform distribution
        workload_gen = new workload_generator(cfg.expt_range, &cfg.workload);
        txns = workload_gen->Gen();
        
        // One worker thread
        total_op_count = 0;
        for (auto w : cfg.workload) { 
                total_op_count = w.op_count; 
        }
        worker = new Worker(map, txns, total_op_count, cfg.async, cfg.window_size);

        // Run workers
        auto start = get_time::now();
        worker->run();
        pthread_join(*worker->get_pthread_id(), NULL);
        auto end = get_time::now();
        // Measure duration 
        chrono::duration<double> diff = end - start;

        // Write to output file
        cout << total_op_count / diff.count() << endl;
        write_output(cfg.client_id, total_op_count / diff.count());
        
        // Free
        delete worker;
        delete map;
        delete workload_gen;
}

int main(int argc, char** argv) {

        config_parser cfg_parser;
        config cfg = cfg_parser.get_config(argc, argv);

        do_experiment(cfg);

        return 0;
}
