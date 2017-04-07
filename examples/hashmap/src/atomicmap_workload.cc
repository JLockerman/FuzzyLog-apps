#include <atomicmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <atomicmap.h>

void ycsb_insert::Run() {
        uint32_t key, value;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        m_map->put(key, value, m_color, m_dep_color);
        // increment executed count
        m_context->inc_num_executed();
}

void ycsb_insert::AsyncRun() {
        uint32_t key, value;
        assert(m_map != NULL);
        assert(m_dep_color == NULL);            // XXX: in AtomicMap, do not support cross-color put at the moment
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        m_map->async_put(key, value, m_color);
}

void ycsb_insert::AsyncRemoteRun() {
        assert(false);
}

bool ycsb_insert::TryAsyncStronglyConsistentRun() {
        assert(false);
}

void ycsb_insert::AsyncWeaklyConsistentRun() {
        assert(false);
}

void ycsb_read::Run() {
        uint32_t key;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);

        m_map->get(key);
}

void ycsb_read::AsyncRun() {
        uint32_t key;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);

        m_map->get(key);
}

void ycsb_read::AsyncRemoteRun() {
        assert(false);
}

bool ycsb_read::TryAsyncStronglyConsistentRun() {
        assert(false);
}

void ycsb_read::AsyncWeaklyConsistentRun() {
        assert(false);
}

Txn** atomicmap_workload_generator::Gen() {
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

                                if (op_type == "get") {
                                        txns[i] = new ycsb_read(m_map, color, start, end, m_context, Txn::optype::GET);

                                } else if (op_type == "put") {
                                        struct colors* dep_color = (*m_workload)[j].has_dependency ? &(*m_workload)[j].dep_color : NULL;
                                        bool is_strong = (*m_workload)[j].is_strong;
                                        txns[i] = new ycsb_insert(m_map, color, dep_color, start, end, m_context, Txn::optype::PUT, is_strong);
                                }
                                allocations[j] = allocations[j] - 1;
                                i++;
                                break;
                        }
                }
        }

        return txns;
}
