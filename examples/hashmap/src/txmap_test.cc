#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <txmap_tester.h>
#include <signal.h>

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;

void write_output(uint32_t client_id, std::string output) {
        std::ofstream result_file; 
        result_file.open(std::to_string(client_id) + ".txt", std::ios::app | std::ios::out);
        result_file << output; 
        result_file.close();        
}

void measure_fn(TXMapTester *w, uint64_t duration, std::vector<uint64_t> &executed_results, std::vector<uint64_t> &committed_results)
{
        uint64_t start_executed, end_executed;
        uint64_t start_committed, end_committed;
        
        end_executed = w->get_num_executed();
        end_committed = w->get_num_committed();
        for (auto i = 0; i < duration; ++i) {
                start_executed = end_executed; 
                start_committed = end_committed;
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
                end_executed = w->get_num_executed();        
                end_committed = w->get_num_committed();
                std::cout << i << " executed : " << end_executed << " - " << start_executed << " = " << end_executed - start_executed << std::endl;
                std::cout << i << " committed: " << end_committed << " - " << start_committed << " = " << end_committed - start_committed << std::endl;
                executed_results.push_back(end_executed - start_executed);
                committed_results.push_back(end_committed - start_committed);
        }
}

void wait_signal(txmap_config cfg)
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

void do_experiment(txmap_config cfg) {
        uint64_t total_op_count;
        TXMap *map;
        txmap_workload_generator *workload_gen;
        Txn** txns;
        TXMapTester* worker;
        std::atomic<bool> flag;
        std::vector<uint64_t> executed_results;
        std::vector<uint64_t> committed_results;

        // Total operation count
        total_op_count = 0;
        assert(cfg.workload.size() == 1);
        workload_config &w = cfg.workload[0];
        total_op_count = w.op_count;

        // Context 
        uint64_t local_key_range_start;
        uint64_t local_key_range_end;
        uint64_t remote_key_range_start;
        uint64_t remote_key_range_end;
        assert(w.first_color.numcolors == 2);
        ColorID local_color = w.first_color.mycolors[0];
        local_key_range_start = local_color * cfg.expt_range;
        local_key_range_end = local_key_range_start + cfg.expt_range;
        ColorID remote_color = w.first_color.mycolors[1];
        remote_key_range_start = remote_color * cfg.expt_range;
        remote_key_range_end = remote_key_range_start + cfg.expt_range;
        TXMapContext ctx(cfg.window_size, cfg.rename_percent, local_key_range_start, local_key_range_end, remote_key_range_start, remote_key_range_end);

        // Fuzzymap
        map = new TXMap(&cfg.log_addr, &cfg.workload, &ctx, cfg.replication);

        // Generate append workloads: uniform distribution
        workload_gen = new txmap_workload_generator(&ctx, map, &cfg.workload);
        txns = workload_gen->Gen();
        
        // One worker thread
        flag = true;
        worker = new TXMapTester(&ctx, map, &flag, txns, total_op_count, cfg.async, cfg.window_size, cfg.expt_duration, cfg.txn_rate);

        // Synchronize clients
        //wait_signal(cfg);

        // Run workers
        worker->run();
        
        // Measure
        if (cfg.expt_duration > 0) {
                std::this_thread::sleep_for(std::chrono::seconds(5));
                measure_fn(worker, cfg.expt_duration, executed_results, committed_results);
                // Stop worker
                flag = false;
        }
        while (!ctx.is_finished()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Give more time to reader thread
        std::this_thread::sleep_for(std::chrono::seconds(3));
        std::stringstream ss;
        ss << "total executed              : " << ctx.get_num_executed() << std::endl;
        ss << "total executed (local only) : " << ctx.get_num_local_only_txns() << std::endl;
        ss << "total executed (distributed): " << ctx.get_num_dist_txns() << std::endl;
        ss << "total committed             : " << ctx.get_num_committed() << std::endl;
        ss << "total aborted               : " << ctx.get_num_aborted() << std::endl;
        double abort_rate = (double)ctx.get_num_aborted() / ctx.get_num_executed() * 100.0;
        ss.precision(8);
        ss << "execution time              : " << ctx.get_execution_time() << std::endl; 
        ss << "tput                        : " << ctx.get_throughput() << std::endl; 
        ss << "gput                        : " << ctx.get_goodput() << std::endl; 
        ss.precision(2);
        ss << "abort rate                  : " << abort_rate << std::endl; 
        
        // Print
        std::cout << ss.str();

        // Write to file
        write_output(cfg.client_id, ss.str());

        // Wait until worker finishes
        worker->join();

        // Free
        delete worker;
        delete map;
        delete workload_gen;
}

int main(int argc, char** argv) {

        txmap_config_parser cfg_parser;
        txmap_config cfg = cfg_parser.get_config(argc, argv);

        do_experiment(cfg);

        return 0;
}
