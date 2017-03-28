#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <worker.h>
#include <signal.h>

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;


void write_output(uint32_t client_id, std::vector<uint64_t>& results) {
        std::ofstream result_file; 
        result_file.open(std::to_string(client_id) + ".txt", std::ios::app | std::ios::out);
        for (auto r : results) {
                result_file << r << "\n"; 
        }
        result_file.close();        
}

void measure_fn(Worker *w, uint64_t duration, std::vector<uint64_t> &results)
{
        uint64_t start_iters, end_iters;
        
        end_iters = w->get_num_executed();
        for (auto i = 0; i < duration; ++i) {
                start_iters = end_iters; 
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                end_iters = w->get_num_executed();        
                std::cout << i << " measured: " << end_iters << " - " << start_iters << " = " << end_iters - start_iters << std::endl;
                results.push_back(end_iters - start_iters);
        }
}

void do_experiment(config cfg) {
        uint32_t total_op_count;
        HashMap *map;
        workload_generator *workload_gen;
        Txn** txns;
        Worker* worker;
        std::atomic<bool> flag;
        std::vector<uint64_t> results;

        // Total operation count
        total_op_count = 0;
        for (auto w : cfg.workload) { 
                total_op_count += w.op_count;
        }
        Context ctx;    // Can be used to share info between Worker and Txns

        // Fuzzymap
        map = new HashMap(&cfg.log_addr, &cfg.workload);

        // Generate append workloads: uniform distribution
        workload_gen = new workload_generator(&ctx, map, cfg.expt_range, &cfg.workload);
        txns = workload_gen->Gen();
        
        // One worker thread
        flag = true;
        worker = new Worker(&ctx, map, &flag, txns, total_op_count, cfg.async, cfg.window_size);

        // Run workers
        worker->run();
        
        // Measure
        std::this_thread::sleep_for(std::chrono::seconds(5));  
        measure_fn(worker, cfg.expt_duration, results);

        // Stop worker
        flag = false;

        while (!ctx.is_finished()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Wait until worker finishes
        worker->join();

        // Write to output file
        write_output(cfg.client_id, results);
        
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
