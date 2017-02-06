#ifndef		WORKLOAD_GENERATOR_H_
#define 	WORKLOAD_GENERATOR_H_

#include <cstdint>
#include <cstdlib>
class workload_generator {
private:
	 uint64_t	_range;
	
public:
	workload_generator(uint64_t range); 	
	uint64_t gen_next();
};

#endif 		// WORKLOAD_GENERATOR_H_ 
