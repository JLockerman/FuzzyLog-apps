package client;

public class WriteID {

	public long 			_p1;
	public long				_p2;
	private WriteID			_next;
	
	public WriteID(long p1, long p2) {
		_p1 = p1;
		_p2 = p2;
		_next = null;
	}
	
	public WriteID() {
		_p1 = 0;
		_p2 = 0;
		_next = null;
	}
	
	public void initialize(long p1, long p2) {
		_p1 = p1;
		_p2 = p2;
	}
	
	public void set_next(WriteID wid) {
		_next = wid;
	}
	
	public int hashCode() {
		
		int p1_front = (int)(_p1 >> 32);
		int p1_back = (int)_p1;
		
		int p2_front = (int)(_p2 >> 32);
		int p2_back = (int)_p2;
		
		int hash = 17;
		hash = hash*31 + p1_front;
		hash = hash*31 + p1_back;
		hash = hash*31 + p2_front;
		hash = hash*32 + p2_back;
		return hash;
	}
	
	public boolean equals(Object obj) {
		WriteID other = (WriteID)obj;
		if (other != null) {
			return other._p1 == _p1 && other._p2 == _p2;
		}
		return false;
	}
}
