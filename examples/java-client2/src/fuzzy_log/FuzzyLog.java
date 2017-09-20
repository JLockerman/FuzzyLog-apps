package fuzzy_log;

import com.sun.jna.*;
import c_link.*;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Arrays;

import java.lang.Iterable;

import fuzzy_log.ReadHandle;
import fuzzy_log.WriteHandle;

public class FuzzyLog {

    private Pointer handle;
    public FuzzyLog(String[] servers, int... intersting_colors) {
        colors.ByReference interesting = Utils.new_colors(intersting_colors);
        StringArray arr = new StringArray(servers);
        this.handle = FuzzyLogLibrary.new_dag_handle_with_skeens(new size_t(servers.length), arr, interesting);
    }
/*
    public FuzzyLog(String[] server_heads, String[] server_tails, int... intersting_colors) {
        int num_servers;
        if (server_heads.length <= server_tails.length) {
            num_servers = server_heads.length;
        } else {
            num_servers = server_tails.length;
        }
        colors.ByReference interesting = Utils.new_colors(intersting_colors);
        this.handle = FuzzyLogLibrary.new_dag_handle_with_replication(new size_t(num_servers), server_heads, server_tails, interesting);
    }
*/
    public ReadHandleAndWriteHandle split() {
        reader_and_writer.ByValue raw = FuzzyLogLibrary.split_dag_handle(this.handle);
        this.handle = Pointer.NULL;
        ReadHandle read = new ReadHandle(raw.reader.share(0));
        WriteHandle write = new WriteHandle(raw.writer.share(0));
        return new ReadHandleAndWriteHandle(read, write);
    }
/*
    public write_id async_append(int[] append_colors, byte[] buffer) {
        colors.ByReference inhabits = Utils.new_colors(append_colors);
        return FuzzyLogLibrary.do_append(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), inhabits, null, (byte)1);
    }

    public write_id async_append_causal(int[] append_colors, int[] causal_colors, byte[] buffer) {
        colors.ByReference inhabits = Utils.new_colors(append_colors);
        colors.ByReference happens_after = Utils.new_colors(append_colors);
        return FuzzyLogLibrary.async_simple_causal_append(handle, ByteBuffer.wrap(buffer), new size_t(buffer.length), inhabits, happens_after);
	}

	public write_id wait_any_append() {
        return FuzzyLogLibrary.wait_for_any_append(handle);
	}

	public write_id try_wait_any_append() {
        return FuzzyLogLibrary.try_wait_for_any_append(handle);
	}

	public void snapshot() {
        FuzzyLogLibrary.snapshot(handle);
	}

	public Events get_events() {
        return new Events();
    }

    public void close() {
        FuzzyLogLibrary.close_dag_handle(handle);
        handle = Pointer.NULL;
    }

    public class Events implements Iterator<Event>, Iterable<Event> {
        private boolean done = false;
        private boolean got_next = false;
        private Event next = FuzzyLog.Event.NO_EVENT;

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
            this.next = FuzzyLog.Event.NO_EVENT;
            return ret;
        }

        private void fetch_next() {
            size_tByReference data_size = new size_tByReference();
            size_tByReference locs_read = new size_tByReference();
            get_next_val next_val = FuzzyLogLibrary.get_next2(FuzzyLog.this.handle, data_size, locs_read);
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
    }*/

    public static class ReadHandleAndWriteHandle {
        public final ReadHandle reader;
        public final WriteHandle writer;

        ReadHandleAndWriteHandle(ReadHandle readHandle, WriteHandle writeHandle) {
            this.reader = readHandle;
            this.writer = writeHandle;
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
