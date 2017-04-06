#pragma once

#include <sstream>
#include <vector>
#include <string>
#include <cstdint>
#include <cassert>
#include <iostream>
#include <getopt.h>
#include <stdlib.h>
#include <unordered_map>

static struct option long_options[] = {
  {"log_addr", required_argument, NULL, 0},
  {"proxy_port", required_argument, NULL, 1},
  {"num_clients", required_argument, NULL, 2},
  {NULL, no_argument, NULL, 3},
};

struct config {
	std::vector<std::string> 		log_addr;
	uint16_t				proxy_port;
	uint32_t 				num_clients;
}; 

class config_parser {
private:
	enum OptionCode {
		LOG_ADDR = 0,
		PROXY_PORT=1,
		NUM_CLIENTS=2,
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
		    _arg_map.count(PROXY_PORT) == 0 || 
		    _arg_map.count(NUM_CLIENTS) == 0) {
		        std::cerr << "Missing one or more params\n";
		        std::cerr << "--" << long_options[LOG_ADDR].name << "\n";
		        std::cerr << "--" << long_options[PROXY_PORT].name << "\n";
		        std::cerr << "--" << long_options[NUM_CLIENTS].name << "\n";
		        exit(-1);
		}
	
		ret.proxy_port = (uint16_t)atoi(_arg_map[PROXY_PORT]);
		ret.num_clients = (uint32_t)atoi(_arg_map[NUM_CLIENTS]);

		std::string log_addresses;
		log_addresses.assign(_arg_map[LOG_ADDR]);
		std::stringstream addr_s(log_addresses);	
		std::string item;
			
		while (getline(addr_s, item, ',')) 
			ret.log_addr.push_back(item);
		
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
