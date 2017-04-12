#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <capmap_tester.h>
#include <signal.h>
#include <capmap.h>

extern "C" {
        #include "fuzzy_log.h"
}

#define FAKE_NETWORK_PARTITION_STARTS_AT        6       // in seconds
#define FAKE_NETWORK_PARTITION_ENDS_AT          8       // in seconds
#define MEASUREMENT_INTERVAL                    100     // in milliseconds

void write_output(uint32_t client_id, std::vector<uint64_t>& results) {
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


void measure_fn(CAPMap *m, CAPMapTester *w, uint64_t duration, std::vector<workload_config>& workload, std::vector<uint64_t> &results,
                std::vector<latency_footprint> &put_latencies, std::vector<latency_footprint> &get_latencies)
{
        uint64_t start_iters, end_iters;
        
        bool needs_to_be_aware_of_partition = 
                                m->get_protocol_version() == CAPMap::ProtocolVersion::VERSION_1 || 
                                (m->get_protocol_version() == CAPMap::ProtocolVersion::VERSION_2 && m->get_role() == "secondary");

        bool put_workload_found = false;
        for (auto w : workload) {
                if (w.op_type == "put") {
                        put_workload_found = true;
                        break;
                }
        }
        
        end_iters = w->get_num_executed();
        for (auto i = 0; i < duration * 1000 / MEASUREMENT_INTERVAL; ++i) {
                start_iters = end_iters; 
                std::this_thread::sleep_for(std::chrono::milliseconds(MEASUREMENT_INTERVAL));
                end_iters = w->get_num_executed(); 
                if (put_workload_found) {
                        std::cout << i << " measured: " << end_iters << " - " << start_iters << " = " << end_iters - start_iters << std::endl;
                        results.push_back(end_iters - start_iters);
                }

                if (needs_to_be_aware_of_partition) {
                        // XXX: emulate network partition
                        if (i == FAKE_NETWORK_PARTITION_STARTS_AT * 1000 / MEASUREMENT_INTERVAL) {
                                m->set_network_partition_status(CAPMap::PartitionStatus::PARTITIONED);
                        }
                        else if (i == FAKE_NETWORK_PARTITION_ENDS_AT * 1000 / MEASUREMENT_INTERVAL) {
                                m->set_network_partition_status(CAPMap::PartitionStatus::HEALING);
                        }
                }
        }
        w->get_put_latencies(put_latencies);
        w->get_get_latencies(get_latencies);

        std::cout << "put latencies:" << put_latencies.size() << std::endl;
        std::cout << "get latencies:" << get_latencies.size() << std::endl;
}

void do_experiment(capmap_config cfg) {
        uint32_t total_op_count;
        CAPMap *map;
        capmap_workload_generator *workload_gen;
        Txn** txns;
        CAPMapTester* worker;
        std::atomic<bool> flag;
        std::vector<uint64_t> results;
        std::vector<latency_footprint> put_latencies;
        std::vector<latency_footprint> get_latencies;

        // Total operation count
        total_op_count = 0;
        for (auto w : cfg.workload) { 
                total_op_count += w.op_count;
        }
        Context ctx;    // Can be used to share info between CAPMapTester and Txns

        // Fuzzymap
        map = new CAPMap(&cfg.log_addr, &cfg.workload, (CAPMap::ProtocolVersion)cfg.protocol, cfg.role);

        // Generate append workloads: uniform distribution
        workload_gen = new capmap_workload_generator(&ctx, map, cfg.expt_range, &cfg.workload);
        txns = workload_gen->Gen();
        
        // One worker thread
        flag = true;
        worker = new CAPMapTester(&ctx, map, &flag, txns, total_op_count, cfg.async, cfg.window_size, cfg.txn_rate);

        // Run workers
        worker->run();
        
        // Measure
        //std::this_thread::sleep_for(std::chrono::seconds(5));
        measure_fn(map, worker, cfg.expt_duration, cfg.workload, results, put_latencies, get_latencies);

        // Stop worker
        flag = false;

        while (!ctx.is_finished()) {
                std::this_thread::sleep_for(std::chrono::seconds(1));
        }

        // Wait until worker finishes
        worker->join();
 
        // Write to output file
        if (put_latencies.size() > 0) {
                std::string put_latency_output_suffix = "_put_latency.txt";
                write_latency(cfg.client_id, put_latency_output_suffix, put_latencies);
        }
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

        capmap_config_parser cfg_parser;
        capmap_config cfg = cfg_parser.get_config(argc, argv);

        do_experiment(cfg);

        return 0;
}
