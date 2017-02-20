#include <workload.h>
#include <cassert>
#include <random>
#include <iostream>

void ycsb_insert::Run(HashMap *map) {
        uint32_t key, value;
        assert(map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        map->put(key, value, m_color);
}

void ycsb_insert::AsyncRun(HashMap *map) {
        uint32_t key, value;
        assert(map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        map->async_put(key, value, m_color);
}

Txn** workload_generator::Gen() {
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
                                txns[i] = new ycsb_insert(0, m_range, &(*m_workload)[j].color);
                                allocations[j] = allocations[j] - 1;
                                i++;
                                break;
                        }
                }
        }

        return txns;
}
