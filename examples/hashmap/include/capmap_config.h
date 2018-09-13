#pragma once

#include <config.h>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <sstream>
#include <vector>
#include <getopt.h>
#include <stdlib.h>
#include "util.h"

static struct option long_options[] = {
        {"log_addr",            required_argument, NULL, 0},
        {"expt_range",          required_argument, NULL, 1},
        {"expt_duration",       required_argument, NULL, 2},
        {"num_clients",         required_argument, NULL, 3},
        {"client_id",           required_argument, NULL, 4},
        {"protocol",            required_argument, NULL, 5},
        {"role",                optional_argument, NULL, 6},
        {"workload",            required_argument, NULL, 7},
        {"txn_rate",            optional_argument, NULL, 8},
        {"async",               optional_argument, NULL, 9},
        {"window_size",         optional_argument, NULL, 10},
        {"replication",         optional_argument, NULL, 11},
        {NULL,                  no_argument,       NULL, 12},
};

struct capmap_config {
	std::vector<std::string>        log_addr;
	uint64_t 		        expt_range;
        uint32_t                        expt_duration;
	uint8_t 		        num_clients;
	uint8_t 		        client_id;
        uint8_t                         protocol;
        std::string                     role;
	std::vector<workload_config>    workload;
        uint32_t                        txn_rate;
        bool                            async;
        uint32_t                        window_size;
        bool                            replication;
};

class capmap_config_parser {
private:
	enum OptionCode {
		LOG_ADDR        = 0,
		EXPT_RANGE      = 1,
		EXPT_DURATION   = 2,
		NUM_CLIENTS     = 3,
		CLIENT_ID       = 4,
		PROTOCOL        = 5,
		ROLE            = 6,
		WORKLOAD        = 7,
                TXN_RATE        = 8,
		ASYNC           = 9,
		WINDOW_SIZE     = 10,
                REPLICATION     = 11,
	};

	bool 					_init;
	std::unordered_map<int, char*> 		_arg_map;

	void read_args(int argc, char **argv)
	{
		assert(_init == false);

		int index = -1;
		while (getopt_long_only(argc, argv, "", long_options, &index) != -1) {
			if (index != -1 && _arg_map.count(index) == 0) { /* correct argument */
				_arg_map[index] = optarg;
			} else if (index == -1) { /* Unknown argument */
				std::cerr << "Error. Unknown argument\n";
				exit(-1);
			} else { /* Duplicate argument */
				std::cerr << "Error. Duplicate argument\n";
				exit(-1);
			}
		}
		_init = true;
	}

	capmap_config create_config()
	{
		assert(_init == true);
		capmap_config ret;

		if (_arg_map.count(LOG_ADDR) == 0 ||
		    _arg_map.count(EXPT_RANGE) == 0 ||
		    _arg_map.count(EXPT_DURATION) == 0 ||
		    _arg_map.count(NUM_CLIENTS) == 0 ||
		    _arg_map.count(CLIENT_ID) == 0 ||
		    _arg_map.count(PROTOCOL) == 0 ||
		    _arg_map.count(WORKLOAD) == 0) {
		        std::cerr << "Missing one or more params\n";
		        std::cerr << "--" << long_options[LOG_ADDR].name << "\n";
		        std::cerr << "--" << long_options[EXPT_RANGE].name << "\n";
		        std::cerr << "--" << long_options[EXPT_DURATION].name << "\n";
		        std::cerr << "--" << long_options[NUM_CLIENTS].name << "\n";
		        std::cerr << "--" << long_options[CLIENT_ID].name << "\n";
		        std::cerr << "--" << long_options[PROTOCOL].name << "\n";
		        std::cerr << "[--" << long_options[ROLE].name << "]\n";
		        std::cerr << "--" << long_options[WORKLOAD].name << "\n";
		        std::cerr << "[--" << long_options[TXN_RATE].name << "]\n";
		        std::cerr << "[--" << long_options[ASYNC].name << "]\n";
		        std::cerr << "[--" << long_options[WINDOW_SIZE].name << "]\n";
		        std::cerr << "[--" << long_options[REPLICATION].name << "]\n";
			exit(-1);
		}

                if (_arg_map.count(ASYNC) == 0 && _arg_map.count(WINDOW_SIZE) > 0) {
                        std::cerr << "Error. --" << long_options[WINDOW_SIZE].name << " should be set with --" << long_options[ASYNC].name << " turned on\n";
                        exit(-1);
                }

                // log_addr
                ret.log_addr = split(std::string(_arg_map[LOG_ADDR]), ',');
                // expt_range
		ret.expt_range = static_cast<uint64_t>(atoi(_arg_map[EXPT_RANGE]));
                // expt_duration
		ret.expt_duration = static_cast<uint32_t>(atoi(_arg_map[EXPT_DURATION]));
                // num_clients
		ret.num_clients = static_cast<uint8_t>(atoi(_arg_map[NUM_CLIENTS]));
                // client_id
		ret.client_id = static_cast<uint8_t>(atoi(_arg_map[CLIENT_ID]));
                // protocol
		ret.protocol = static_cast<uint8_t>(atoi(_arg_map[PROTOCOL]));
                if (ret.protocol == 2 && _arg_map.count(ROLE) == 0) {
                        std::cerr << "Error. --" << long_options[ROLE].name << " should be set with protocol 2\n";
                        exit(-1);
                }
                // role
                if (_arg_map.count(ROLE) > 0) {
                        ret.role = std::string(_arg_map[ROLE]);
                }

                // workload
                std::vector<std::string> workloads = split(std::string(_arg_map[WORKLOAD]), ',');
                for (auto w : workloads) {
                        // parse: {get|put}@color_name[{-|~}color_name]=op_count
                        // color_name = color_value[:color_value]...
                        std::vector<std::string> pair = split(w, '=');
                        assert(pair.size() == 2);

                        // operation type = {get|put}
                        std::vector<std::string> op_type_and_color = split(pair[0], '@');
                        assert(op_type_and_color.size() == 2);
                        std::string op_type = op_type_and_color[0];
                        if (op_type != "get" && op_type != "put") {
                                std::cerr << "Error. --" << long_options[WORKLOAD].name << " only allows get|put\n";
                                exit(-1);
                        }

                        // color scheme = color_name[{-|~}color_name]
                        ColorSpec color = {};
                        bool is_strong = false;
                        std::string& color_scheme = op_type_and_color[1];

                        // consistency level
                        // color_name-color_name = strong
                        // color_name~color_name = weak
                        bool has_dependency = color_scheme.find("-") != std::string::npos
                            || color_scheme.find("~") != std::string::npos;

                        if (has_dependency) {
                                is_strong = color_scheme.find("-") != std::string::npos;
                                if (!is_strong)
                                        assert(color_scheme.find("~") != std::string::npos);

                                std::vector<std::string> color_dep = split(color_scheme, is_strong ? '-' : '~');
                                assert(color_dep.size() <= 2);

                                // depedency color
                                string_to_colors(color_dep[0], &color.local_chain);
                                uint64_t *remote_color = new uint64_t[1];
                                color.remote_chains = remote_color;
                                color.num_remote_chains = 1;
                                string_to_colors(color_dep[1], color.remote_chains);


                        } else {
                                // no depedency color
                                string_to_colors(color_scheme, &color.local_chain);
                        }

                        // op_count
                        uint64_t op_count = static_cast<uint64_t>(stoi(pair[1]));

                        // workload
                        workload_config wc;
                        wc.op_type = op_type;
                        wc.colors = color;
                        wc.has_dependency = has_dependency;
                        wc.is_strong = is_strong;
                        wc.op_count = op_count;
                        ret.workload.push_back(wc);
                }
                // txn_rate
                if (_arg_map.count(TXN_RATE) > 0)
                        ret.txn_rate = static_cast<uint32_t>(atoi(_arg_map[TXN_RATE]));
                else
                        ret.txn_rate = 0;

                // async
                ret.async = _arg_map.count(ASYNC) > 0;
                // window_size
                if (ret.async && _arg_map.count(WINDOW_SIZE) > 0)
                        ret.window_size = static_cast<uint32_t>(atoi(_arg_map[WINDOW_SIZE]));
                else if (ret.async)
                        ret.window_size = DefaultWindowSize;
                else
                        ret.window_size = 0;

                // replication
                ret.replication = _arg_map.count(REPLICATION) > 0;

		return ret;
	}

        void string_to_colors(std::string& str, uint64_t* out)
        {
                std::vector<std::string> color_list = split(str, ':');
                assert(color_list.size() == 1);
                // out->numcolors = color_list.size();
                // out->mycolors = new ColorID[color_list.size()];
                for (auto i = 0; i < color_list.size(); ++i) {
                        // out->mycolors[i] = (ColorID)stoi(color_list[i]);
                    *out = (uint64_t)stoull(color_list[i]);
                }
        }



public:
	capmap_config_parser()
	{
		_init = false;
		_arg_map.clear();
	}

	capmap_config get_config(int argc, char **argv)
	{
		read_args(argc, argv);
		return create_config();
	}
};
