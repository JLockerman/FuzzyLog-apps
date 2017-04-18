#include <txmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <txmap.h>

void read_write_txn::Run() {
        assert(false);
}

write_id read_write_txn::AsyncRun() {
        assert(m_map != NULL);
        uint64_t from_key, to_key;
       
        // uniform distribution 
        from_key = rand() % (m_local_key_range_end - m_local_key_range_start);
        to_key = rand() % (m_remote_key_range_end - m_remote_key_range_start);

        m_map->execute_move_txn(from_key, to_key);

        m_context->inc_num_executed();
}

write_id read_write_txn::AsyncRemoteRun() {
        assert(false);
}

write_id read_write_txn::AsyncStronglyConsistentRun() {
        assert(false);
}

write_id read_write_txn::AsyncWeaklyConsistentRun() {
        assert(false);
}

Txn** txmap_workload_generator::Gen() {
        uint64_t i;
        uint64_t total_op_count;
        double r;
        
        uint64_t local_key_range_start;
        uint64_t local_key_range_end;
        uint64_t remote_key_range_start;
        uint64_t remote_key_range_end;

        // percent of single operations
        assert(m_workload->size() == 1);
        workload_config &w = m_workload->at(0);
        total_op_count = w.op_count;
        local_key_range_start = 0;        
        local_key_range_end = w.expt_range;
        remote_key_range_start = w.expt_range + 1;
        remote_key_range_end = w.expt_range * 2;


        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_op_count);

        // make transactions
        for (i = 0; i < total_op_count; i++) {
                struct colors* color = &w.first_color; 
                uint64_t start = 0; 

                txns[i] = new read_write_txn(m_map, color, local_key_range_start, local_key_range_end, remote_key_range_start, remote_key_range_end, m_context);
        }

        return txns;
}
