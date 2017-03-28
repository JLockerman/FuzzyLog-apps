#pragma once

#include <atomic>
#include <or_set.h>
#include <vector>

class or_set_getter {
private:
	std::atomic<uint64_t> 		_num_elapsed;		
	or_set 				*_set;	
	std::atomic<bool> 		_quit;
	
	void do_measurement(std::vector<double> &samples, int interval, int duration); 
	void do_run(std::vector<uint64_t> *gets_per_snapshot);

public:
	or_set_getter(or_set *set);
	void run(std::vector<double> &samples, std::vector<uint64_t> &gets_per_snapshot, int interval, int duration);
};
