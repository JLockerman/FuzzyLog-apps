#pragma once 

#include <cstdint>
#include <cstdlib>
class workload_generator {
private:
	 uint64_t	_range;
	
public:
	workload_generator(uint64_t range); 	
	uint64_t gen_next();
};
