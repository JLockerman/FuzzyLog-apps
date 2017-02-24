#pragma once

#include <chrono>
#include <vector>
#include <set>

struct tester_request {
	std::chrono::system_clock::time_point 		_start_time;
	std::chrono::system_clock::time_point 		_end_time;	
};

class async_tester {
private:
	uint32_t _window_sz;

protected:	
	virtual void issue_request(tester_request *rq) = 0;
	virtual void wait_requests(std::set<tester_request*> &done_set) = 0; 

	uint32_t try_get_pending();
public:
	async_tester(uint32_t window_sz);
	std::chrono::milliseconds run(std::vector<tester_request*> requests);
};
