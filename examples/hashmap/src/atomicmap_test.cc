#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <atomicmap_tester.h>
#include <signal.h>

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;


void write_throughput(uint32_t client_id, std::vector<uint64_t>& results) {
        std::ofstream result_file; 
        result_file.open(std::to_string(client_id) + ".txt", std::ios::app | std::ios::out);
        for (auto r : results) {
                result_file << r << "\n"; 
        }
        result_file.close();        
}

void write_latency(uint32_t client_id, std::string& suffix, std::vector<latency_footprint>& latencies) {
        std::ofstream result_file; 
        result_file.open(std::to_string(client_id) + suffix, std::ios::app | std::ios::out);
        for (auto l : latencies) {
                result_file << l.m_issue_time.time_since_epoch().count() << " " << l.m_latency << "\n"; 
        }
        result_file.close();        
}

void measure_fn(AtomicMapTester *w, uint64_t duration, std::vector<uint64_t> &results)
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

void wait_signal(atomicmap_config cfg)
{
        char buffer[DELOS_MAX_DATA_SIZE];
        size_t buf_sz;
        struct colors c, depends, interested;   
        auto num_received = 0;

        ColorID sig_color[1];   
        sig_color[0] = cfg.num_clients + 1;
        c.mycolors = sig_color;
        c.numcolors = 1;
        buf_sz = 1;
        
        interested = c;

        /* Server ips for handle. */
        DAGHandle *handle = NULL;
        if (cfg.replication) {
                assert (cfg.log_addr.size() > 0 && cfg.log_addr.size() % 2 == 0);
                size_t num_chain_servers = cfg.log_addr.size() / 2;
                const char *chain_server_head_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_head_ips[i] = cfg.log_addr[i].c_str();
                }
                const char *chain_server_tail_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_tail_ips[i] = cfg.log_addr[num_chain_servers+i].c_str();
                }
                handle = new_dag_handle_with_replication(num_chain_servers, chain_server_head_ips, chain_server_tail_ips, &c);

        } else {
                size_t num_servers = cfg.log_addr.size();
                assert(num_servers > 0);
                const char *server_ips[num_servers];
                for (auto i = 0; i < num_servers; ++i) {
                        server_ips[i] = cfg.log_addr[i].c_str();
                }
                handle = new_dag_handle_with_skeens(num_servers, server_ips, &c);
        }

        depends.mycolors = NULL;
        depends.numcolors = 0;

        append(handle, buffer, buf_sz, &c, &depends);
        while (num_received < cfg.num_clients) {
                snapshot(handle);
                while (true) {
                        get_next(handle, buffer, &buf_sz, &c);
                        assert(c.numcolors == 0 || c.numcolors == 1);   
                        if (c.numcolors == 1) {
                                //assert(c.mycolors[0] == 1);
                                num_received += 1;
                                free(c.mycolors);
                        } else {
                                break;
                        }
                }
        }       
        close_dag_handle(handle);
}

void do_experiment(atomicmap_config cfg) {
        uint64_t total_op_count;
        AtomicMap *map;
        atomicmap_workload_generator *workload_gen;
        Txn** txns;
        AtomicMapTester* worker;
        std::atomic<bool> flag;
        std::vector<uint64_t> results;
        std::vector<latency_footprint> put_latencies;
        std::vector<latency_footprint> get_latencies;

        // Total operation count
        total_op_count = 0;
        for (auto w : cfg.workload) { 
                total_op_count += w.op_count;
        }
        Context ctx;    // Can be used to share info between AtomicMapTester and Txns

        // Fuzzymap
        map = new AtomicMap(&cfg.log_addr, &cfg.workload, cfg.replication);

        // Generate append workloads: uniform distribution
        workload_gen = new atomicmap_workload_generator(&ctx, map, cfg.expt_range, &cfg.workload);
        txns = workload_gen->Gen();
        
        // One worker thread
        flag = true;
        worker = new AtomicMapTester(&ctx, map, &flag, txns, total_op_count, cfg.async, cfg.window_size, cfg.expt_duration, cfg.txn_rate);

        // Synchronize clients
        wait_signal(cfg);

        // Run workers
        worker->run();
        
        // Measure
        if (cfg.expt_duration > 0) {
                std::this_thread::sleep_for(std::chrono::seconds(5));  
                measure_fn(worker, cfg.expt_duration, results);
                // Stop worker
                flag = false;
        }

        while (!ctx.is_finished()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Wait until worker finishes
        worker->join();
 
        // Write throughput to output file
        if (results.size() > 0)
                write_throughput(cfg.client_id, results);

        // Write latency to output file
        worker->get_put_latencies(put_latencies);
        std::cout << "put latencies:" << put_latencies.size() << std::endl;
        if (put_latencies.size() > 0) {
                std::string put_latency_output_suffix = "_put_latency.txt";
                write_latency(cfg.client_id, put_latency_output_suffix, put_latencies);
        }
        worker->get_get_latencies(get_latencies);
        std::cout << "get latencies:" << get_latencies.size() << std::endl;
        if (get_latencies.size() > 0) {
                std::string get_latency_output_suffix = "_get_latency.txt";
                write_latency(cfg.client_id, get_latency_output_suffix, get_latencies);
        }
       
        // Free
        delete worker;
        delete map;
        delete workload_gen;
}

int main(int argc, char** argv) {

        atomicmap_config_parser cfg_parser;
        atomicmap_config cfg = cfg_parser.get_config(argc, argv);

        do_experiment(cfg);

        return 0;
}
