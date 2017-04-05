package client;

public class WriteID {

	private int 		_p1;
	private int			_p2;
	
	public WriteID(int p1, int p2) {
		_p1 = p1;
		_p2 = p2;
	}
	
	public WriteID() {
		_p1 = 0;
		_p2 = 0;
	}
	
	public void initialize(int p1, int p2) {
		_p1 = p1;
		_p2 = p2;
	}
	
	public int hashCode() {
		return (_p1*71 ^ _p2);
	}
	
	public boolean equals(Object obj) {
		WriteID other = (WriteID)obj;
		if (other != null) {
			return other._p1 == _p1 && other._p2 == _p2;
		}
		return false;
	}
}
