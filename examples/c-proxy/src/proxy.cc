#include <proxy.h>
#include <iostream>
#include <cstring> 

void fuzzy_proxy::receive_data_sz(int socket, char *buf, size_t sz)
{
	size_t received = 0;
	while (received < sz) {
		auto diff = recv(socket, &buf[received], sz - received, 0);	
		received += diff;	
	}
	assert(received == sz);
} 

write_id fuzzy_proxy::do_async_append()
{
	uint32_t num_colors = _request._append_colors.size();
	ColorID colors[num_colors];	

	struct colors append_colors;
	append_colors.mycolors = colors;
	append_colors.numcolors = num_colors;

	auto i = 0;
	for (auto c : _request._append_colors) {
		colors[i] = (uint8_t)c;
		i += 1;
	}

	return async_append(_handle, _request._payload, _request._payload_size, &append_colors, NULL);  
}

void fuzzy_proxy::do_try_wait_any(std::deque<write_id> &wid_list)
{
	assert(wid_list.size() == 0);
	for (auto i = 0; i < _max_try_wait; ++i) {
		auto wid = try_wait_for_any_append(_handle);
		if (wid_equality{} (WRITE_ID_NIL, wid))  
			break;
		wid_list.push_back(wid);
	}
}


write_id fuzzy_proxy::do_wait_any()
{
	return wait_for_any_append(_handle);
}

void fuzzy_proxy::do_snapshot()
{
	snapshot(_handle);	
}

void proxy_request::initialize(char *buf)
{
	_append_colors.clear();
	_depend_colors.clear();		
	
	uint32_t *int_buf = (uint32_t*)buf;
	int num_append_colors, num_depend_colors;
	
	assert(ntohl(int_buf[0]) <= GET_NEXT);   	
	
	_opcode = (opcode)(ntohl(int_buf[0]));	
	if (_opcode == ASYNC_APPEND) {
		num_append_colors = ntohl(int_buf[1]);
		num_depend_colors = ntohl(int_buf[2]); 
	
		uint8_t *color_buf = (uint8_t*)(&int_buf[3]);
		for (auto i = 0; i < num_append_colors; ++i) {
			_append_colors.insert(color_buf[i]);
		}	
		
		color_buf = &color_buf[num_append_colors];
		for (auto i = 0; i < num_depend_colors; ++i) {
			_depend_colors.insert(color_buf[i]);
		}
		auto offset = sizeof(uint32_t)*3 + (num_append_colors + num_depend_colors)*sizeof(uint8_t);	
		uint32_t *payload_sz = (uint32_t*)&buf[offset];				
		_payload_size = ntohl(payload_sz[0]);
		_payload = (char *)&payload_sz[1];
	}
	
}

fuzzy_proxy::fuzzy_proxy(DAGHandle *handle, uint16_t port)
{
	_handle = handle;
	_max_try_wait = 100;

	/* Set up server's socket */
	_socket_fd = socket(AF_INET, SOCK_STREAM, 0);
	assert(_socket_fd >= 0);
	memset(&_server_addr, 0x0, sizeof(_server_addr));	
	
	_server_addr.sin_family = AF_INET;	
	_server_addr.sin_port = htons(port);	
	_server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");	
	
	auto success = bind(_socket_fd, (struct sockaddr*)&_server_addr, sizeof(_server_addr)); 
	assert(success == 0); 
	
	success = listen(_socket_fd, 20);	
	assert(success == 0);	
	
	_client_addrlen = sizeof(_client_addr);
	_client_fd = accept(_socket_fd, (struct sockaddr*)&_client_addr, &_client_addrlen); 		
}

fuzzy_proxy::~fuzzy_proxy()
{
	close(_client_fd);
	close(_socket_fd);
}

void fuzzy_proxy::serialize_wait_any_response(write_id wid)
{
	write_id *wid_buf = (write_id *)_buffer;
	*wid_buf = wid;
	_buffer_len = sizeof(write_id);
}

void fuzzy_proxy::serialize_try_wait_any_response(const std::deque<write_id> &done_set)
{
	uint32_t num_acks = done_set.size();
	
	uint32_t *buf = (uint32_t *)_buffer;	
	buf[0] = htonl(num_acks);
		
	uint64_t *long_buf = (uint64_t *)(&buf[1]);
	auto i = 0;
	for (auto wid : done_set) {
		long_buf[i] = wid.p1;
		long_buf[i+1] = wid.p2;
		i += 2;	
	}	
	_buffer_len = sizeof(uint32_t) + 2*sizeof(uint64_t)*done_set.size();
}

void fuzzy_proxy::serialize_snapshot_response()
{
	assert(false);
}

void fuzzy_proxy::serialize_get_next_response()
{
	assert(false);
}

void fuzzy_proxy::serialize_async_append_response(write_id wid)
{
	auto int_buf = (uint64_t *)_buffer;
	int_buf[0] = wid.p1;
	int_buf[1] = wid.p2;
	_buffer_len = 2*sizeof(uint64_t);
}

void fuzzy_proxy::run()
{
	std::deque<write_id> write_id_set;
	write_id wid;
	
	while (true) {
		if (recv(_client_fd, _buffer, MAX_PROXY_BUF, 0) <= 0)
			break;

		_request.initialize(_buffer);
		
		switch(_request._opcode) {

		case proxy_request::ASYNC_APPEND:
			wid = do_async_append();
			serialize_async_append_response(wid);
			send(_client_fd, _buffer, _buffer_len, 0);
			break;

		case proxy_request::TRY_WAIT_ANY_APPEND:
			write_id_set.clear();
			do_try_wait_any(write_id_set);
			serialize_try_wait_any_response(write_id_set);
			send(_client_fd, _buffer, _buffer_len, 0);
			break;

		case proxy_request::WAIT_ANY_APPEND:
			wid = do_wait_any();
			serialize_wait_any_response(wid);			
			send(_client_fd, _buffer, _buffer_len, 0);
			break;

		case proxy_request::SNAPSHOT:	
			do_snapshot();
			send(_client_fd, _buffer, sizeof(uint32_t), 0);
			break;

		case proxy_request::GET_NEXT:
			/* XXX Yet to be implemented. */
			assert(false);
			break;
		case proxy_request::QUIT:
			return;
		default:
			assert(false);
		}								
	}
}

