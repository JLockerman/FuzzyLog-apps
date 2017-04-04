#include <capmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <capmap.h>

void ycsb_cross_insert::Run() {
        assert(false);
}

void ycsb_cross_insert::AsyncRun() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        return m_map->async_normal_put(key, value, m_color);
}

void ycsb_cross_insert::AsyncRemoteRun() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        return m_map->async_normal_put(key, value, m_dep_color);
}

bool ycsb_cross_insert::TryAsyncStronglyConsistentRun() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        return m_map->async_strong_depend_put(key, value, m_color, m_dep_color);
}

void ycsb_cross_insert::AsyncWeaklyConsistentRun() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        m_map->async_weak_put(key, value, m_color);
}

void ycsb_cross_insert::AsyncPartitioningAppend() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        m_map->async_partitioning_put(key, value, m_color, m_dep_color);     // XXX: append to remote color, weakly dependent on the local color
}

void ycsb_cross_insert::AsyncHealingAppend() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color != NULL);

        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();
        m_map->async_healing_put(key, value, m_dep_color, m_color);     // XXX: append to remote color, weakly dependent on the local color
}

Txn** capmap_workload_generator::Gen() {
        uint32_t i;
        uint32_t total_op_count;
        double r;

        // percent of single operations
        total_op_count = 0;
        for (auto w : *m_workload) {
                total_op_count += w.op_count;
        }
        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_op_count);

        // make transactions
        vector<double> proportions;
        vector<uint32_t> allocations;
        uint32_t n = 0;
        for (auto w : *m_workload) {
                n += w.op_count; 
                double p = (double)n / total_op_count;
                proportions.push_back(p);
                allocations.push_back(w.op_count);
        }

        i = 0;
        while (i < total_op_count) {
                // dice
                r = ((double) rand() / (RAND_MAX));

                for (auto j = 0; j < proportions.size(); j++) {
                        if (proportions[j] > r && allocations[j] > 0) { 
                                std::string op_type = (*m_workload)[j].op_type; 
                                struct colors* color = &(*m_workload)[j].color; 
                                uint32_t start = 0; 
                                uint32_t end = m_range; 

                                if (op_type == "put") {
                                        struct colors* dep_color = (*m_workload)[j].has_dependency ? &(*m_workload)[j].dep_color : NULL;
                                        txns[i] = new ycsb_cross_insert(m_map, color, dep_color, start, end, m_context, Txn::optype::PUT);
                                } else {
                                        assert(false);
                                }
                                allocations[j] = allocations[j] - 1;
                                i++;
                                break;
                        }
                }
        }

        return txns;
}
