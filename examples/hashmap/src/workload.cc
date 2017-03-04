#include <workload.h>
#include <cassert>
#include <random>
#include <iostream>

void ycsb_insert::Run() {
        uint32_t key, value;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        m_map->put(key, value, m_color);
        // increment executed count
        m_context->inc_num_executed();
}

void ycsb_insert::AsyncRun() {
        uint32_t key, value;
        assert(m_map != NULL);
       
        // key = (start, end)
        key = rand() % (m_end - m_start);
        value = rand();

        m_map->async_put(key, value, m_color);
}

new_write_id ycsb_insert::wait_for_any_op() {
        new_write_id nwid = m_map->wait_for_any_put();
        // increment executed count
        m_context->inc_num_executed();
        return nwid;
}

void ycsb_insert::wait_for_all_ops() {
        new_write_id nwid;
        while (true) {
                if ((nwid = wait_for_any_op()) == NEW_WRITE_ID_NIL) break;
        }
}

void ycsb_insert::flush_completed_ops() {
        m_map->flush_completed_puts(); 
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
                                txns[i] = new ycsb_insert(m_map, &(*m_workload)[j].color, 0, m_range, m_context);
                                allocations[j] = allocations[j] - 1;
                                i++;
                                break;
                        }
                }
        }

        return txns;
}
