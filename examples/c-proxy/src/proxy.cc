#include <proxy.h>
#include <cstring> 

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

void proxy_request::initialize(char *buf)
{
	_append_colors.clear();
	_depend_colors.clear();		
	
	uint32_t *int_buf = (uint32_t*)buf;
	
	assert(int_buf[0] <= GET_NEXT);   	
	
	_opcode = (opcode)(int_buf[0]);	
	if (_opcode == ASYNC_APPEND) {
		auto num_append_colors = int_buf[1];
		auto num_depend_colors = int_buf[2]; 
	
		uint8_t *color_buf = (uint8_t*)(&int_buf[3]);
		for (auto i = 0; i < num_append_colors; ++i) {
			_append_colors.insert(color_buf[i]);
		}	
		
		color_buf = &color_buf[num_append_colors];
		for (auto i = 0; i < num_depend_colors; ++i) {
			_depend_colors.insert(color_buf[i]);
		}
	}
}

fuzzy_proxy::fuzzy_proxy(DAGHandle *handle, uint16_t port)
{
	_handle = handle;

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
	
	uint32_t *buf = (uint32_t*)_buffer;	
	buf[0] = num_acks;
		
	write_id *wid_buf = (write_id *)(&buf[1]);
	auto i = 0;
	for (auto wid : done_set) {
		wid_buf[i] = wid;
		i += 1;	
	}	
	
	_buffer_len = sizeof(uint32_t) + sizeof(write_id)*done_set.size();			
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
	write_id *wid_buf = (write_id *)_buffer;
	*wid_buf = wid;
	_buffer_len = sizeof(write_id);	
}

void fuzzy_proxy::run()
{
	std::deque<write_id> write_id_set;
	write_id wid;
	
	while (true) {
		recv(_client_fd, _buffer, MAX_PROXY_BUF, 0);
		_request.initialize(_buffer);
		
		switch(_request._opcode) {

		case proxy_request::ASYNC_APPEND:
			wid = do_async_append();
			serialize_async_append_response(wid);
			break;

		case proxy_request::TRY_WAIT_ANY_APPEND:
			write_id_set.clear();
			do_try_wait_any(write_id_set);
			serialize_try_wait_any_response(write_id_set);
			break;

		case proxy_request::WAIT_ANY_APPEND:
			wid = do_wait_any();
			serialize_wait_any_response(wid);			
			break;

		case proxy_request::SNAPSHOT:	
			/* XXX Yet to be implemented. */
			assert(false);
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

		send(_client_fd, _buffer, _buffer_len, 0);
	}
}

