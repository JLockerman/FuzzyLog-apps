#pragma once

#include <string>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>

static struct option long_options[] = {
  {"log_addr", required_argument, NULL, 0},
  {"expt_duration", required_argument, NULL, 1},
  {"expt_range", required_argument, NULL, 2},
  {"server_id", required_argument, NULL, 3},
  {NULL, no_argument, NULL, 4},
};

struct config {
	std::string 		log_addr;
	uint64_t 		expt_duration;
	uint64_t 		expt_range;
	uint8_t 		server_id;		
	
}; 

class config_parser {
private:
	enum OptionCode {
		LOG_ADDR = 0,
		EXPT_DURATION = 1,
		EXPT_RANGE = 2,
		SERVER_ID = 3,
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
		    _arg_map.count(EXPT_DURATION) == 0 ||
		    _arg_map.count(EXPT_RANGE) == 0 ||
		    _arg_map.count(SERVER_ID) == 0) {

		        std::cerr << "Missing one or more params\n";
		        std::cerr << "--" << long_options[LOG_ADDR].name << "\n";
		        std::cerr << "--" << long_options[EXPT_DURATION].name << "\n";
		        std::cerr << "--" << long_options[EXPT_RANGE].name << "\n";
		        std::cerr << "--" << long_options[SERVER_ID].name << "\n";
			exit(-1);
		}
		
		ret.expt_duration = (uint64_t)atoi(_arg_map[EXPT_DURATION]);		
		ret.expt_range = (uint64_t)atoi(_arg_map[EXPT_RANGE]);
		ret.server_id = (uint8_t)atoi(_arg_map[SERVER_ID]);
		ret.log_addr.assign(_arg_map[LOG_ADDR]);
	
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
