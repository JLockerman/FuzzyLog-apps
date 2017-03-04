#pragma once

#include <string>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <sstream>
#include <vector>
#include <getopt.h>
#include <stdlib.h>
#include "util.h"

extern "C" {
        #include "fuzzy_log.h"
}

#define DEFAULT_WINDOW_SIZE 32

static struct option long_options[] = {
        {"log_addr",            required_argument, NULL, 0},
        {"expt_range",          required_argument, NULL, 1},
        {"expt_duration",       required_argument, NULL, 2},
        {"client_id",           required_argument, NULL, 3},
        {"workload",            required_argument, NULL, 4},
        {"async",               optional_argument, NULL, 5},
        {"window_size",         optional_argument, NULL, 6},
        {NULL,                  no_argument,       NULL, 7},
};

struct workload_config {
        struct colors                   color;
        uint64_t                        op_count;
};

struct config {
	std::vector<std::string>        log_addr;
	uint32_t 		        expt_range;
        uint32_t                        expt_duration;
	uint8_t 		        client_id;
	std::vector<workload_config>    workload;
        bool                            async;
        uint32_t                        window_size;
}; 

class config_parser {
private:
	enum OptionCode {
		LOG_ADDR        = 0,
		EXPT_RANGE      = 1,
		EXPT_DURATION   = 2,
		CLIENT_ID       = 3,
		WORKLOAD        = 4,
		ASYNC           = 5,
		WINDOW_SIZE     = 6,
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
	
	config create_config()
	{
		assert(_init == true);
		config ret;

		if (_arg_map.count(LOG_ADDR) == 0 || 
		    _arg_map.count(EXPT_RANGE) == 0 ||
		    _arg_map.count(EXPT_DURATION) == 0 ||
		    _arg_map.count(CLIENT_ID) == 0 || 
		    _arg_map.count(WORKLOAD) == 0) {
		        std::cerr << "Missing one or more params\n";
		        std::cerr << "--" << long_options[LOG_ADDR].name << "\n";
		        std::cerr << "--" << long_options[EXPT_RANGE].name << "\n";
		        std::cerr << "--" << long_options[EXPT_DURATION].name << "\n";
		        std::cerr << "--" << long_options[CLIENT_ID].name << "\n";
		        std::cerr << "--" << long_options[WORKLOAD].name << "\n";
			exit(-1);
		}

                if (_arg_map.count(ASYNC) == 0 && _arg_map.count(WINDOW_SIZE) > 0) {
                        std::cerr << "Error. --" << long_options[WINDOW_SIZE].name << " should be set with --" << long_options[ASYNC].name << " turned on\n";
                        exit(-1);
                }
		
                // log_addr
                ret.log_addr = split(std::string(_arg_map[LOG_ADDR]), ',');
                // expt_range
		ret.expt_range = (uint32_t)atoi(_arg_map[EXPT_RANGE]);
                // expt_duration
		ret.expt_duration = (uint32_t)atoi(_arg_map[EXPT_DURATION]);
                // client_id
		ret.client_id = (uint8_t)atoi(_arg_map[CLIENT_ID]);
                // workload
                std::vector<std::string> workloads = split(std::string(_arg_map[WORKLOAD]), ',');
                for (auto w : workloads) {
                        // parse: <color[:color2]>=<op_count>                        
                        std::vector<std::string> pair = split(w, '='); 
                        assert(pair.size() == 2); 

                        // color
                        std::vector<std::string> color_list = split(pair[0], ':');
                        struct colors c;
                        c.numcolors = color_list.size();
                        c.mycolors = new ColorID[color_list.size()];
                        for (auto i = 0; i < color_list.size(); ++i) {
                                c.mycolors[i] = (ColorID)stoi(color_list[i]);
                        }

                        // op_count
                        uint32_t op_count = (uint32_t)stoi(pair[1]);

                        // workload
                        workload_config wc;
                        wc.color = c;
                        wc.op_count = op_count; 
                        ret.workload.push_back(wc);
                }
                // async
                ret.async = _arg_map.count(ASYNC) > 0;
                // window_size
                if (ret.async && _arg_map.count(WINDOW_SIZE) > 0)
                        ret.window_size = (uint32_t)atoi(_arg_map[WINDOW_SIZE]);
                else if (ret.async)
                        ret.window_size = DEFAULT_WINDOW_SIZE;
                else
                        ret.window_size = 0;

		return ret;
	}


public:
	config_parser() 
	{
		_init = false;
		_arg_map.clear();
	}

	config get_config(int argc, char **argv)
	{
		read_args(argc, argv);
		return create_config();
	}
};
