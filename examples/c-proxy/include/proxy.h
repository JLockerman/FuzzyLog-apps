#pragma once

#include <set>
#include <deque>
#include <cassert>
#include <stddef.h>

extern "C" {
	#include <fuzzy_log.h>
	#include <sys/socket.h>
	#include <arpa/inet.h>
	#include <unistd.h>
}

#define MAX_PROXY_BUF		4096	

class wid_equality {
public:
	bool operator() (write_id const &wid1, write_id const &wid2) const {
		return (wid1.p1 == wid2.p1) && (wid1.p2 == wid2.p2);
	}
};

class proxy_request {
public:
	typedef enum {
		ASYNC_APPEND=0,
		TRY_WAIT_ANY_APPEND=1,
		WAIT_ANY_APPEND=2,
		SNAPSHOT=3,
		GET_NEXT=4,
		QUIT=5,
	} opcode;	

	opcode			_opcode;
	std::set<ColorID> 	_append_colors;
	std::set<ColorID> 	_depend_colors;			
	char 			*_payload;
	size_t 			_payload_size;
	
	void initialize(char *buf);

	opcode get_opcode();
	const std::set<ColorID>& get_append_colors();
	const std::set<ColorID>& get_depend_colors();
};

class fuzzy_proxy {

private:
	DAGHandle 		*_handle;
	char 			_buffer[MAX_PROXY_BUF];	
	std::size_t 		_buffer_len;
	proxy_request 		_request;
	uint32_t 		_max_try_wait;	
	
	/* Socket specific data */
	int 			_socket_fd;
	int 			_client_fd;
	socklen_t		_client_addrlen;
	struct sockaddr_in 	_server_addr;
	struct sockaddr_in 	_client_addr;		
	
	/* Request handlers */
	write_id do_async_append();
	write_id do_wait_any();
	void do_try_wait_any(std::deque<write_id> &wid_set);	
	void do_snapshot();
	void do_get_next();
	
	/* Response serializers */
	void receive_data_sz(int socket, char *buf, size_t sz);
	void serialize_async_append_response(write_id wid);
	void serialize_wait_any_response(write_id wid);
	void serialize_try_wait_any_response(const std::deque<write_id> &wid_set);
	void serialize_snapshot_response();
	void serialize_get_next_response();

public:	
	fuzzy_proxy(DAGHandle *handle, uint16_t port);
	~fuzzy_proxy();
	void run();
};
