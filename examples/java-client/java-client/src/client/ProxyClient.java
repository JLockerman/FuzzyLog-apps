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
		_output.writeInt(0);
		_output.writeInt(append_colors.length);
		_output.writeInt(0);
		
		_output.write(append_colors);
		_output.writeInt(payload.length);
		_output.write(payload);
		
		_output.flush();
	}
	
	private void deserialize_async_append_response(WriteID wid) throws IOException {
		long part1 = _input.readLong();
		long part2 = _input.readLong();
		wid.initialize(part1, part2);
	}
	
	private void serialize_wait_any() throws IOException {
		_output.writeInt(2);
		_output.flush();
	}
	
	private void deserialize_wait_any_response(WriteID wid) throws IOException {
		long part1 = _input.readLong();
		long part2 = _input.readLong();
		wid.initialize(part1, part2);
	}
	
	private void serialize_try_wait_any() throws IOException {
		_output.writeInt(1);
		_output.flush();
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
	
	private void serialize_snapshot() throws IOException {
		_output.writeInt(3);
		_output.flush();
	}
	
	private void deserialize_snapshot() throws IOException {
		_input.readInt();
	}
	
	private void serialize_get_next() throws IOException {
		_output.writeInt(4);
		_output.flush();
	}
	
	private boolean deserialize_get_next_response(byte[] data_buf, byte[] color_buf, int[] num_results) throws IOException {
		int buf_sz = _input.readInt();
		int colors_sz = _input.readInt();
		
		num_results[0] = buf_sz;
		num_results[1] = colors_sz;
		if (colors_sz == 0) {
			return false;
		}
		
		_input.read(data_buf, 0, buf_sz);
		_input.read(color_buf, 0, colors_sz);
		return true;
	}
	
	public ProxyClient(int server_port) throws IOException {
		String ip = "127.0.0.1";
		_server_port = server_port;
		_socket = new Socket(ip, _server_port);
		_input = new DataInputStream(new BufferedInputStream(_socket.getInputStream()));
		_output = new DataOutputStream(new BufferedOutputStream(_socket.getOutputStream()));
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
	
	public void snapshot() throws IOException {
		serialize_snapshot();
		deserialize_snapshot();
	}
	
	public boolean get_next(byte[] data, byte[] colors, int[] num_results) throws IOException {
		serialize_get_next();
		return deserialize_get_next_response(data, colors, num_results);
	}
}
