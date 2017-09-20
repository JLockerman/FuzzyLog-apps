package fuzzy_log;

import com.sun.jna.*;
import c_link.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Arrays;

import java.lang.Iterable;

public class WriteHandle {

    private final Pointer handle;

    WriteHandle(Pointer handle) {
        this.handle = handle;
    }

    public write_id async_append(int append_color, byte[] buffer) {
        return FuzzyLogLibrary.wh_async_append(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), append_color);
    }

    public write_id async_append(int append_color, ByteBuffer buffer) {
        return FuzzyLogLibrary.wh_async_append(handle, buffer, new size_t(buffer.remaining()), append_color);
    }

    public write_id async_append(int[] append_colors, byte[] buffer) {
        if(append_colors.length == 1){
            return FuzzyLogLibrary.wh_async_append(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), append_colors[0]);
        } else {
            colors.ByReference inhabits = Utils.new_colors(append_colors);
            return FuzzyLogLibrary.wh_async_multiappend(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), inhabits);
        }
    }

    public write_id no_remote_multiappend(int[] append_colors, byte[] buffer) {
        colors.ByReference inhabits = Utils.new_colors(append_colors);
        return FuzzyLogLibrary.wh_async_no_remote_multiappend(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), inhabits);
    }

	public write_id wait_any_append() {
        return FuzzyLogLibrary.wh_wait_for_any_append(handle);
	}

	public void flush_completed_appends() {
        FuzzyLogLibrary.wh_flush_completed_appends(handle);
	}
}
