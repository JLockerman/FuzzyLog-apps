package client;

public class Allocator {
	
	private WriteID 		_freelist;
	private WriteID[]		_buf;
	
	public Allocator(int size) {
		_buf = new WriteID[size];
		
		for (int i = 0; i < size; ++i) {
			if (i == size - 1) {
				_buf[i].set_next(null);
			} else {
				_buf[i].set_next(_buf[i+1]);
			}
		}
		
		_freelist = _buf[0];
	}
}
