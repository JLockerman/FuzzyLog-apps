#include <proxy.h>
#include <config.h>

DAGHandle* create_fuzzylog_handle(config cfg)
{
	auto num_servers = cfg.log_addr.size();
	
	const char *server_ips[num_servers];
	for (auto i = 0; i < num_servers; ++i) 
		server_ips[i] = cfg.log_addr[i].c_str();
	
	auto num_colors = cfg.num_clients;
	ColorID colors[num_colors];
	for (auto i = 0; i < num_colors; ++i) {
		colors[i] = (uint8_t)i;
	}

	struct colors app_colors;
	app_colors.numcolors = num_colors;
	app_colors.mycolors = colors;

	auto handle = new_dag_handle_with_skeens(num_servers, server_ips, &app_colors);
	return handle;
}

int main(int argc, char **argv)
{
	config_parser cfg_parser;
	auto cfg = cfg_parser.get_config(argc, argv);
	
	auto handle = create_fuzzylog_handle(cfg);
	fuzzy_proxy proxy(handle, cfg.proxy_port);
		
	proxy.run();
	close_dag_handle(handle);
	return 0;
}
