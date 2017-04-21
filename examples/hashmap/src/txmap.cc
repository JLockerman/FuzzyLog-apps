#include <txmap.h>
#include <iostream>
#include <fstream>
#include <cassert>
#include <unistd.h>

TXMap::TXMap(std::vector<std::string>* log_addr, std::vector<workload_config>* workload, Context* context, bool replication): BaseMap(log_addr, replication), m_synchronizer(NULL) {
        std::vector<ColorID> interesting_colors;
        get_interesting_colors(workload, interesting_colors);
        init_synchronizer(log_addr, interesting_colors, context, replication);

        // init color
        init_op_color(interesting_colors);
}

TXMap::~TXMap() {
        if (m_synchronizer != NULL)
                m_synchronizer->join();
}

void TXMap::get_interesting_colors(std::vector<workload_config>* workload, std::vector<ColorID>& interesting_colors) {
        assert(workload->size() == 1);
        workload_config &w = workload->at(0);
        assert(w.first_color.numcolors == 2);           // Assumes two servers 
        uint32_t i;
        for (i = 0; i < w.first_color.numcolors; i++) {
                interesting_colors.push_back(w.first_color.mycolors[i]);
        }
}

void TXMap::init_synchronizer(std::vector<std::string>* log_addr, std::vector<ColorID>& interesting_colors, Context* context, bool replication) {
        m_synchronizer = new TXMapSynchronizer(log_addr, interesting_colors, context, m_fuzzylog_client, replication);
        m_synchronizer->run();
}

void TXMap::init_op_color(std::vector<ColorID>& interesting_colors) {
        // for local txn
        m_local_txn_color.numcolors = 1;
        m_local_txn_color.mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)));
        m_local_txn_color.mycolors[0] = interesting_colors[0];

        // for dist txn
        m_dist_txn_color.numcolors = interesting_colors.size();
        m_dist_txn_color.mycolors = static_cast<ColorID*>(malloc(sizeof(ColorID)*interesting_colors.size()));

        uint32_t i;
        for (i = 0; i < interesting_colors.size(); i++) {
                m_dist_txn_color.mycolors[i] = interesting_colors[i];
        }
}

uint64_t TXMap::get(uint64_t key) {
        return m_synchronizer->get(key);        // get stale data
}

void TXMap::serialize_commit_record(txmap_commit_node *commit_node, char* out, size_t* out_size) {
        txmap_commit_node *buf_node;
        buf_node = reinterpret_cast<txmap_commit_node*>(out);
        buf_node->node.node_type        = commit_node->node.node_type;
        buf_node->commit_version        = commit_node->commit_version;
        buf_node->read_key              = commit_node->read_key;
        buf_node->read_key_version      = commit_node->read_key_version;
        buf_node->write_key             = commit_node->write_key;
        buf_node->remote_write_key      = commit_node->remote_write_key;
        *out_size = sizeof(txmap_commit_node);
}

void TXMap::execute_update_txn(uint64_t key) {
        txmap_commit_node commit_node;
        // node type
        commit_node.node.node_type = txmap_node::NodeType::COMMIT_RECORD;
        // commit version
        commit_node.commit_version = 0;         // will be filled when validation
        // read/writeset
        commit_node.read_key = key;
        commit_node.read_key_version = get(key);
        commit_node.write_key = key;
        commit_node.remote_write_key = 0;
        // Make payload
        size_t size = 0;
        serialize_commit_record(&commit_node, m_buf, &size);
        // Append commit record
        //async_no_remote_append(m_fuzzylog_client, m_buf, size, &m_local_txn_color, NULL, 0);
        async_append(m_fuzzylog_client, m_buf, size, &m_local_txn_color, NULL);
}

void TXMap::execute_rename_txn(uint64_t from_key, uint64_t to_key) {
        txmap_commit_node commit_node;
        // node type
        commit_node.node.node_type = txmap_node::NodeType::COMMIT_RECORD;
        // commit version
        commit_node.commit_version = 0;         // will be filled when validation
        // read/writeset
        commit_node.read_key = from_key;
        commit_node.read_key_version = get(from_key);
        commit_node.write_key = from_key;
        commit_node.remote_write_key = to_key;
        // Make payload
        size_t size = 0;
        serialize_commit_record(&commit_node, m_buf, &size);
        // Append commit record
        async_no_remote_append(m_fuzzylog_client, m_buf, size, &m_dist_txn_color, NULL, 0);
        //async_append(m_fuzzylog_client, m_buf, size, &m_dist_txn_color, NULL);
}

void TXMap::log(txmap_set* rset, txmap_set* wset) {
  //    std::ofstream result_file; 
  //    result_file.open("txns.txt", std::ios::app | std::ios::out);
  //    result_file << "========== " << "APPEND COMMIT RECORD" << " ==========" << std::endl; 
  //    result_file << "[R] " << rset->log(); 
  //    result_file << "[W] " << wset->log(); 
  //    result_file.close();        
}
