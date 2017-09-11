package c_link;

import com.sun.jna.ptr.ByReference;
import com.sun.jna.Native;

public class size_tByReference extends ByReference {
    public size_tByReference() {
        super(Native.SIZE_T_SIZE);
    }

    public size_tByReference(long value) {
        super(Native.SIZE_T_SIZE);
        this.getPointer().setLong(0, value);
    }

    public long getValue() {
        return this.getPointer().getLong(0);
    }

    public void setValue(long val) {
        this.getPointer().setLong(0, val);
    }
}