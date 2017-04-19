#include <txmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <txmap.h>

void read_write_txn::Run() {
        assert(m_map != NULL);
        uint64_t from_key, to_key;
       
        // uniform distribution 
        from_key = rand() % (m_local_key_range_end - m_local_key_range_start) + m_local_key_range_start;
        to_key = rand() % (m_remote_key_range_end - m_remote_key_range_start) + m_remote_key_range_start;
        m_map->execute_move_txn(from_key, to_key);
}

write_id read_write_txn::AsyncRun() {
        assert(false);
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
        
        uint64_t local_key_range_start;
        uint64_t local_key_range_end;
        uint64_t remote_key_range_start;
        uint64_t remote_key_range_end;

        // percent of single operations
        assert(m_workload->size() == 1);
        workload_config &w = m_workload->at(0);
        total_op_count = w.op_count;

        assert(w.first_color.numcolors == 2);
        ColorID local_color = w.first_color.mycolors[0];
        local_key_range_start = local_color * m_key_range;
        local_key_range_end = local_key_range_start + m_key_range;

        ColorID remote_color = w.first_color.mycolors[1];
        remote_key_range_start = remote_color * m_key_range;
        remote_key_range_end = remote_key_range_start + m_key_range;

        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_op_count);

        // make transactions
        for (i = 0; i < total_op_count; i++) {
                struct colors* color = &w.first_color; 
                txns[i] = new read_write_txn(m_map, color, local_key_range_start, local_key_range_end, remote_key_range_start, remote_key_range_end, m_context, Txn::optype::PUT);
        }

        return txns;
}
