#include <workload_generator.h>

workload_generator::workload_generator(uint64_t range)
{
	_range = range;
}

uint64_t workload_generator::gen_next()
{
	return ((uint64_t)rand()) % _range;
}
