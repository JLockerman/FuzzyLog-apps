#include <tester.h>
#include <fuzzy_log.h>
#include <iostream>

async_tester::async_tester(uint32_t window_sz)
{
	_window_sz = window_sz;
}

uint32_t async_tester::try_get_pending() 
{
	std::set<tester_request*> done_rqs;
	
	done_rqs.clear();
	wait_requests(done_rqs);
	
	auto end_time = std::chrono::system_clock::now();
	for (auto rq : done_rqs) 
		rq->_end_time = end_time;
//	std::cerr << "Done requests: " << done_rqs.size() << "\n";
	return done_rqs.size();
}

std::chrono::milliseconds async_tester::run(std::vector<tester_request*> requests)
{
	auto num_pending = 0;
	std::set<tester_request*> done_rqs;
	
	auto start_time = std::chrono::system_clock::now();	
	for (auto rq : requests) {
		while (num_pending == _window_sz) 
			num_pending -= try_get_pending();
			
		rq->_start_time = std::chrono::system_clock::now();
		issue_request(rq);	
		num_pending += 1;
		
		num_pending -= try_get_pending();
	}	
	auto end_time = std::chrono::system_clock::now();
	std::cerr << "Done!\n";
	return std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time); 
}
