#include <txmap_workload.h>
#include <cassert>
#include <random>
#include <iostream>
#include <txmap.h>

void read_write_txn::Run() {
        assert(m_map != NULL);
        uint64_t from_key, to_key;
        TXMapContext *ctx = static_cast<TXMapContext*>(m_context);
       
        // uniform distribution 
        from_key = rand() % (ctx->m_local_key_range_end - ctx->m_local_key_range_start) + ctx->m_local_key_range_start;
        if (ctx->do_rename_txn()) {
                to_key = rand() % (ctx->m_remote_key_range_end - ctx->m_remote_key_range_start) + ctx->m_remote_key_range_start;
                m_map->execute_rename_txn(from_key, to_key);

        } else {
                m_map->execute_update_txn(from_key);
        }
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
        
        // percent of single operations
        assert(m_workload->size() == 1);
        workload_config &w = m_workload->at(0);
        total_op_count = w.op_count;

        Txn **txns = (Txn**)malloc(sizeof(Txn*) * total_op_count);

        // make transactions
        for (i = 0; i < total_op_count; i++) {
                struct colors* color = &w.first_color; 
                txns[i] = new read_write_txn(m_map, color, m_context, Txn::optype::PUT);
        }

        return txns;
}
