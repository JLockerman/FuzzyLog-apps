package client;

import java.io.*;
import java.net.*;
import java.util.Queue;

public class ProxyClient {

	private int 					_server_port;
	private Socket 					_socket;
	private DataOutputStream 		_output;
	private DataInputStream 		_input;
	
	private void serialize_async_append(byte[] append_colors, byte[] payload) throws IOException {
		
		int msg_sz = 16 + append_colors.length + payload.length;
		_output.writeInt(msg_sz);
		
		_output.writeInt(0);
		_output.writeInt(append_colors.length);
		_output.writeInt(0);
		
		_output.write(append_colors);
		_output.writeInt(payload.length);
		_output.write(payload);
	}
	
	private void deserialize_async_append_response(WriteID wid) throws IOException {
		long part1 = _input.readLong();
		long part2 = _input.readLong();
		wid.initialize(part1, part2);
	}
	
	private void serialize_wait_any() throws IOException {
		_output.writeInt(4);
		_output.writeInt(2);
	}
	
	private void deserialize_wait_any_response(WriteID wid) throws IOException {
		long part1 = _input.readLong();
		long part2 = _input.readLong();
		wid.initialize(part1, part2);
	}
	
	private void serialize_try_wait_any() throws IOException {
		_output.writeInt(4);
		_output.writeInt(1);
	}
	
	private void deserialize_try_wait_any_response(Queue<WriteID> wid_list) throws IOException {
		wid_list.clear();
		int num_acks = _input.readInt();
		
		for (int i = 0; i < num_acks; ++i) {
			long part1 = _input.readLong();
			long part2 = _input.readLong();
			
			WriteID wid = new WriteID(part1, part2);
			wid_list.add(wid);
		}
	}
	
	private void serialize_snapshot() {
		throw new UnsupportedOperationException();
	}
	
	private void serialize_get_next() {
		throw new UnsupportedOperationException();
	}
	
	private void deserialize_get_next_response() {
		throw new UnsupportedOperationException();
	}
	
	public ProxyClient(int server_port) throws IOException {
		String ip = "127.0.0.1";
		_server_port = server_port;
		_socket = new Socket(ip, _server_port);
		_input = new DataInputStream(_socket.getInputStream());
		_output = new DataOutputStream(_socket.getOutputStream());
	}
	
	public void async_append(byte[] append_colors, byte[] buffer, WriteID wid)  throws IOException{
		serialize_async_append(append_colors, buffer);
		deserialize_async_append_response(wid);
	}
	
	public void wait_any_append(WriteID ack_wid) throws IOException {
		serialize_wait_any();
		deserialize_wait_any_response(ack_wid);
	}
	
	public void try_wait_any_append(Queue<WriteID> wid_list) throws IOException {
		serialize_try_wait_any();
		deserialize_try_wait_any_response(wid_list);
	}
	
	public void snapshot() {
		serialize_snapshot();
	}
	
	public void get_next() {
		serialize_get_next();
		deserialize_get_next_response();
	}
}
