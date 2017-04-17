#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <chrono>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <txmap_config.h>
#include <signal.h>

extern "C" {
        #include "fuzzy_log.h"
}

using namespace std;
using ns = chrono::nanoseconds;
using get_time = chrono::system_clock;


void wait_signal(txmap_config cfg)
{
        char buffer[DELOS_MAX_DATA_SIZE];
        size_t buf_sz;
        struct colors c, depends, interested;   
        auto num_received = 0;

        ColorID sig_color[1];   
        sig_color[0] = cfg.num_clients + 1;
        c.mycolors = sig_color;
        c.numcolors = 1;
        buf_sz = 1;
        
        interested = c;

        /* Server ips for handle. */
        DAGHandle *handle = NULL;
        if (cfg.replication) {
                assert (cfg.log_addr.size() > 0 && cfg.log_addr.size() % 2 == 0);
                size_t num_chain_servers = cfg.log_addr.size() / 2;
                const char *chain_server_head_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_head_ips[i] = cfg.log_addr[i].c_str();
                }
                const char *chain_server_tail_ips[num_chain_servers]; 
                for (auto i = 0; i < num_chain_servers; i++) {
                        chain_server_tail_ips[i] = cfg.log_addr[num_chain_servers+i].c_str();
                }
                handle = new_dag_handle_with_replication(num_chain_servers, chain_server_head_ips, chain_server_tail_ips, &c);

        } else {
                size_t num_servers = cfg.log_addr.size();
                assert(num_servers > 0);
                const char *server_ips[num_servers];
                for (auto i = 0; i < num_servers; ++i) {
                        server_ips[i] = cfg.log_addr[i].c_str();
                }
                handle = new_dag_handle_with_skeens(num_servers, server_ips, &c);
        }

        depends.mycolors = NULL;
        depends.numcolors = 0;

        append(handle, buffer, buf_sz, &c, &depends);
        while (num_received < cfg.num_clients) {
                snapshot(handle);
                while (true) {
                        get_next(handle, buffer, &buf_sz, &c);
                        assert(c.numcolors == 0 || c.numcolors == 1);   
                        if (c.numcolors == 1) {
                                //assert(c.mycolors[0] == 1);
                                num_received += 1;
                                free(c.mycolors);
                        } else {
                                break;
                        }
                }
        }       
        close_dag_handle(handle);
}

void do_experiment(txmap_config cfg) {
}

int main(int argc, char** argv) {

        txmap_config_parser cfg_parser;
        txmap_config cfg = cfg_parser.get_config(argc, argv);

        do_experiment(cfg);

        return 0;
}
