package fuzzy_log;

import com.sun.jna.*;
import c_link.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Arrays;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.lang.Iterable;

public class ReadHandle {

    private final Pointer handle;

    ReadHandle(Pointer handle) {
        this.handle = handle;
    }

	public void snapshot() {
        FuzzyLogLibrary.rh_snapshot(handle);
	}

	public Events get_events() {
        return new Events();
    }

    public ReadHandle.Objects snapshot_and_get_objects() {
        FuzzyLogLibrary.rh_snapshot(handle);
        return new ReadHandle.Objects();
    }

    public Buffers snapshot_and_get_buffer() {
        FuzzyLogLibrary.rh_snapshot(handle);
        return new ReadHandle.Buffers();
    }

    public class Events implements Iterator<Event>, Iterable<Event> {
        private boolean done = false;
        private boolean got_next = false;
        private Event next = ReadHandle.Event.NO_EVENT;

        @Override
        public boolean hasNext() {
            if(this.done) { return false; }
            if(this.got_next) { return true; }
            this.fetch_next();
            return this.got_next;
        }

        @Override
        public Event next() {
            if(this.done) { throw new NoSuchElementException(); }
            if(!this.got_next) { this.fetch_next(); }
            if(this.done) { throw new NoSuchElementException(); }
            assert(this.got_next);
            this.got_next = false;
            Event ret = this.next;
            this.next = ReadHandle.Event.NO_EVENT;
            return ret;
        }

        private void fetch_next() {
            size_tByReference data_size = new size_tByReference();
            size_tByReference locs_read = new size_tByReference();
            get_next_val next_val = FuzzyLogLibrary.rh_get_next2(ReadHandle.this.handle, data_size, locs_read);
            if(locs_read.getValue() == 0) {
                this.done = true;
                this.got_next = false;
                return;
            }

            byte[] data = next_val.data.getByteArray(0, (int)data_size.getValue());

            fuzzy_log_location[] locs = (fuzzy_log_location[])next_val.locs.toArray((int)locs_read.getValue());
            Location[] locations = new Location[locs.length];
            for(int i = 0; i < locs.length; i++) locations[i] = new Location(locs[i]);
            //fuzzy_log_location[] locations = Arrays.copyOf(locs, locs.length);
            this.next = new Event(data, locations);
            this.got_next = true;
            return;
        }

        @Override
        public Iterator<Event> iterator() {
            return this;
        }
    }

    public static class Event {
        public static final Event NO_EVENT = new Event(new byte[0], new Location[0]);

        public final Location[] locations;
        public final byte[] data;

        Event(byte[] data, Location[] locations) {
            this.data = data;
            this.locations = locations;
        }
    }

    public class Objects implements Iterator<Object>, Iterable<Object> {
        private boolean done = false;
        private boolean got_next = false;
        private Object next = null;

        @Override
        public boolean hasNext() {
            if(this.done) { return false; }
            if(this.got_next) { return true; }
            this.fetch_next();
            return this.got_next;
        }

        @Override
        public Object next() {
            if(this.done) { throw new NoSuchElementException(); }
            if(!this.got_next) { this.fetch_next(); }
            if(this.done) { throw new NoSuchElementException(); }
            assert(this.got_next);
            this.got_next = false;
            Object ret = this.next;
            this.next = null;
            return ret;
        }

        private void fetch_next() {
            size_tByReference data_size = new size_tByReference();
            size_tByReference locs_read = new size_tByReference();
            get_next_val next_val = FuzzyLogLibrary.rh_get_next2(ReadHandle.this.handle, data_size, locs_read);
            if(locs_read.getValue() == 0) {
                this.done = true;
                this.got_next = false;
                return;
            }

            ByteBuffer data = next_val.data.getByteBuffer(0, (int)data_size.getValue());
            try {
                this.next = new ObjectInputStream(new ByteBufferInputStream(data)).readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            this.got_next = true;
            return;
        }

        @Override
        public Iterator<Object> iterator() {
            return this;
        }
    }

    public class Buffers implements Iterator<ByteBuffer>, Iterable<ByteBuffer> {
        private boolean done = false;
        private boolean got_next = false;
        private ByteBuffer next = null;

        @Override
        public boolean hasNext() {
            if(this.done) { return false; }
            if(this.got_next) { return true; }
            this.fetch_next();
            return this.got_next;
        }

        @Override
        public ByteBuffer next() {
            if(this.done) { throw new NoSuchElementException(); }
            if(!this.got_next) { this.fetch_next(); }
            if(this.done) { throw new NoSuchElementException(); }
            assert(this.got_next);
            this.got_next = false;
            ByteBuffer ret = this.next;
            this.next = null;
            return ret;
        }

        private void fetch_next() {
            size_tByReference data_size = new size_tByReference();
            size_tByReference locs_read = new size_tByReference();
            get_next_val next_val = FuzzyLogLibrary.rh_get_next2(ReadHandle.this.handle, data_size, locs_read);
            if(locs_read.getValue() == 0) {
                this.done = true;
                this.got_next = false;
                return;
            }

            this.next = next_val.data.getByteBuffer(0, (int)data_size.getValue());

            this.got_next = true;
            return;
        }

        @Override
        public Iterator<ByteBuffer> iterator() {
            return this;
        }
    }

    public static class Location {
        public final int color;
        public final int entry;

        Location(fuzzy_log_location loc) {
            this.color = loc.color;
            this.entry = loc.entry;
        }

        @Override
        public String toString() {
            return "{color = " + this.color + ", entry = " + this.entry + "}";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;

            if(!(obj instanceof Location)) { return false; }
            Location other = (Location)obj;
            return this.color == other.color && this.entry == other.entry;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + color;
            result = prime * result + entry;
            return result;
        }
    }

    private final static class ByteBufferInputStream extends java.io.InputStream {
		private final ByteBuffer buffer;

		ByteBufferInputStream(ByteBuffer b) {
			buffer = b;
        }

        @Override
        public final int available() {
            return buffer.remaining();
        }

        @Override
		public final int read(byte[] b) {

			return read(b, 0, b.length);
		}

        @Override
		public final int read(byte[] b, int off, int len) {
            if(!buffer.hasRemaining()) {
                return -1;
            }
            int read;
            if(this.buffer.remaining() < len) {
                read = this.buffer.remaining();
            } else {
                read = len;
            }
            this.buffer.get(b, off, read);
			return read;
		}

        @Override
		public final int read() {
            if(!buffer.hasRemaining()) {
                return -1;
            }
			return this.buffer.get();
        }

        @Override
        public final long skip(long n) throws IOException {
            int newPosition = (int)(this.buffer.position() + n);
            if(newPosition > buffer.limit()) {
                newPosition = buffer.limit();
            }
            int skipped = newPosition - this.buffer.position();
            this.buffer.position(newPosition);
            return (long)skipped;

        }
	}

    /*public static class WriteId {
        public final long p1;
        public final long p2;

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null) return false;

            if(!(obj instanceof WriteId)) { return false; }
            WriteId other = (WriteId)obj;
            return this.p1 == other.p1 && this.entry == other.entry;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + color;
            result = prime * result + entry;
            return result;
        }
    }*/
}
