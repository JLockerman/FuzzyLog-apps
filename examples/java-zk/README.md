### Building
First build java-client by running ant within the java-client directory.

After that, switch to java-zk and run:

`ant compile`

### Running 

If the fuzzylog deployment is running on port 12345, run the c-proxy and point it at the log:

(in the c_proxy directory:)

`build/c_proxy --log_addr 127.0.0.1:12345 --proxy_port 12344 --colors 1,2`

Once the c-proxy is running, run:

`ant -Dhostname=localhost -Dport=12344 -Dcolor=1 -Dothercolor=2 -Dtesttype=0 run`
