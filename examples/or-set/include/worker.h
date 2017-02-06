#pragma once

#include <atomic>
#include <or_set.h>
#include <string>
#include <vector>

extern "C" {
	#include <fuzzy_log.h>
}

class worker {
private:
	std::atomic<uint64_t> 		_iterations;
	or_set 				*_or_set;
	std::string 			_log_addr;
	struct colors 			*_color;
	DAGHandle			*_log_client;
	uint8_t 			_proc_id;

public:
	worker(std::string log_addr, uint8_t proc_id);
	uint64_t num_iterations();
	void run_expt(const std::vector<uint64_t> &keys);
};
