package control;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import client.ProxyClient;
import client.WriteID;

public class Start {

	private ProxyClient 					_client;
	private Map<WriteID, Integer>			_pending_appends;
	private int 							_window_sz;
	
	public Start(int port, int window_sz) throws IOException {
		_client = new ProxyClient(port);
		_pending_appends = new HashMap<WriteID, Integer>();
	}
	
	public void test_proxy() throws IOException {
		int num_requests = 100000;
		int num_pending = 0;
		
		byte[] payload = new byte[4];
		payload[0] = (byte)0xFF;
		payload[1] = (byte)0xFF;
		payload[2] = (byte)0xFF;
		payload[3] = (byte)0xFF;
		
		byte[] colors = new byte[1];
		colors[0] = (byte)0x1;
		
		for (int i = 0; i < num_requests; ++i) {
			WriteID wid = new WriteID();
			_client.async_append(colors, payload, wid);
			_pending_appends.put(wid,  i);
			num_pending += 1;
			
			if (num_pending == _window_sz) {
				WriteID ack_wid = new WriteID();
				_client.wait_any_append(ack_wid);
				_pending_appends.remove(ack_wid);
				num_pending -= 1;
			}
		}
		
		while (num_pending != 0) {
			WriteID ack_wid = new WriteID();
			_client.wait_any_append(ack_wid);
			_pending_appends.remove(ack_wid);
			num_pending -= 1;
		}
		
		assert(_pending_appends.size() == 0);
	}
	
	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		
		try {
			Start st = new Start(port, 32);
			st.test_proxy();
		} catch (IOException e) {
			System.err.println("Network error!");
		}
	}
}
