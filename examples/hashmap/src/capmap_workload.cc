#include <capmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <capmap.h>

void ycsb_cross_insert::Run() {
        assert(false);
}

write_id ycsb_cross_insert::AsyncRun() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_normal_put(key, value, m_color);
        m_context->mark_started(wid, start_time); 
        return wid;
}

write_id ycsb_cross_insert::AsyncRemoteRun() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_normal_put(key, value, m_dep_color);
        m_context->mark_started(wid, start_time); 
        return wid;
}

write_id ycsb_cross_insert::AsyncStronglyConsistentRun() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_strong_depend_put(key, value, m_color, m_dep_color);
        m_context->mark_started(wid, start_time); 
        return wid;
}

write_id ycsb_cross_insert::AsyncWeaklyConsistentRun() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_weak_put(key, value, m_color);
        m_context->mark_started(wid, start_time); 
        return wid;
}

write_id ycsb_cross_insert::AsyncPartitioningAppend() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_partitioning_put(key, value, m_color, m_dep_color);     // XXX: append to remote color, weakly dependent on the local color
        m_context->mark_started(wid, start_time); 
        return wid;
}

write_id ycsb_cross_insert::AsyncHealingAppend() {
        uint64_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        auto start_time = std::chrono::system_clock::now();
        write_id wid = m_map->async_healing_put(key, value, m_dep_color, m_color);     // XXX: append to remote color, weakly dependent on the local color
        m_context->mark_started(wid, start_time); 
        return wid;
}

void ycsb_cross_read::Run() {
        uint64_t key;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);

        m_map->get(key);
}

write_id ycsb_cross_read::AsyncRun() {
        assert(false);
}

write_id ycsb_cross_read::AsyncRemoteRun() {
        assert(false);
}

write_id ycsb_cross_read::AsyncStronglyConsistentRun() {
        assert(false);
}

write_id ycsb_cross_read::AsyncWeaklyConsistentRun() {
        assert(false);
}


Txn** capmap_workload_generator::Gen() {
        uint64_t i;
        uint64_t total_op_count;
        double r;

        // percent of single operations
        total_op_count = 0;
        for (auto w : *m_workload) {
                total_op_count += w.op_count;
        }
        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_op_count);

        // make transactions
        vector<double> proportions;
        vector<uint64_t> allocations;
        uint64_t n = 0;
        for (auto w : *m_workload) {
                n += w.op_count; 
                double p = static_cast<double>(n) / total_op_count;
                proportions.push_back(p);
                allocations.push_back(w.op_count);
        }

        i = 0;
        while (i < total_op_count) {
                // dice
                r = static_cast<double>(rand()) / (RAND_MAX);

                for (auto j = 0; j < proportions.size(); j++) {
                        if (proportions[j] > r && allocations[j] > 0) { 
                                assert((*m_workload)[j].has_dependency);
                                std::string op_type = (*m_workload)[j].op_type; 
                                struct colors* local_color = &(*m_workload)[j].first_color; 
                                struct colors* remote_color = &(*m_workload)[j].second_color;
                                uint64_t start = 0; 
                                uint64_t end = m_range; 

                                if (op_type == "put") {
                                        txns[i] = new ycsb_cross_insert(m_map, local_color, remote_color, start, end, m_context, Txn::optype::PUT);

                                } else if (op_type == "get") {
                                        txns[i] = new ycsb_cross_read(m_map, local_color, remote_color, start, end, m_context, Txn::optype::GET);
                                }
                                allocations[j] = allocations[j] - 1;
                                i++;
                                break;
                        }
                }
        }

        return txns;
}
