#include <or_set_getter.h>
#include <thread>

or_set_getter::or_set_getter(or_set *set)
{
	_quit = false;
	_set = set;
	_num_elapsed = 0;
}

void or_set_getter::do_measurement(std::vector<double> &samples,
				  int interval, 
				  int duration)
{
	uint64_t prev, cur;
	prev = _num_elapsed;
	for (auto i = 0; i < duration; i += interval) {
		std::this_thread::sleep_for(std::chrono::seconds(interval));	
		cur = _num_elapsed;
		samples.push_back((cur - prev) / (double)interval);
		prev = cur;
	}	
}

void or_set_getter::do_run(std::vector<uint64_t> *gets_per_snapshot)
{
	uint64_t gets = 0;
	while (_quit == false) {
		if (_set->get_single_remote() == true) {
			_num_elapsed += 1;
			gets += 1;
		} else {
			gets_per_snapshot->push_back(gets);
			gets = 0;
		}
	}
}

void or_set_getter::run(std::vector<double> &samples, std::vector<uint64_t> &gets_per_snapshot, 
			int interval, 
			int duration)
{
	std::thread worker(&or_set_getter::do_run, this, &gets_per_snapshot); 
	do_measurement(samples, interval, duration);
	_quit = true;
	worker.join();
}
