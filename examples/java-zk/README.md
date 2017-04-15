### Building
ant compile

### Running 

If the fuzzylog deployment is running on port 12345, run the c-proxy and point it at the log:

(in the c_proxy directory:)

`build/c_proxy --log_addr 127.0.0.1:12345 --proxy_port 12344 --client_id 1`

Once the c-proxy is running, run:

`ant -Dhostname=localhost -Dport=12344 run`
