#include <worker.h>

worker::worker(std::string log_addr, uint8_t proc_id)
{
	_log_addr = log_addr;
	_color = (struct colors*)malloc(sizeof(struct colors));
        _color->numcolors = 1;
        _color->mycolors = new ColorID[0];
        _color->mycolors[0] = 0; 
	_log_client = new_dag_handle_for_single_server(_log_addr.c_str(), _color);
	_proc_id = proc_id;
	_or_set = new or_set(_log_client, _color, _proc_id);
	_iterations = 0;
}

void worker::run_expt(const std::vector<uint64_t> &keys)
{
	uint64_t i, nkeys;

	nkeys = keys.size();
	for (i = 0; i < nkeys; ++i) {
		_or_set->add(keys[i]);
		_iterations++;
	}
}

uint64_t worker::num_iterations()
{
	return _iterations;
}
