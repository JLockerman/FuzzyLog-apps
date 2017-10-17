package fuzzy_log;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

public final class ProxyHandle implements AutoCloseable {

    private final Process proxy;

    private final Socket appendSocket;
    private final Socket recvSocket;

    private final DataOutputStream append;

    private final DataOutputStream snapshot;
    private final DataInputStream recv;

    private final ByteBufferOutputStream bbos;

    public ProxyHandle(String serverAddr, int port, int... chains) {
        this(serverAddr, port, 0, chains);
    }

    //public ProxyHandle(String serverAddr, int port, long total_clients, int... chains) {
    //    this(new String[] {serverAddr}, port, 0, chains);
    //}

    public ProxyHandle(String serverAddr, int port, long total_clients, int... chains) {
        boolean waitForSync = total_clients > 0;
        try {
            String delosLoc = System.getenv("DELOS_RUST_LOC");
            if(delosLoc == null) throw new NullPointerException("DELOS_RUST_LOC must exist");
            File proxDir = new File(delosLoc, "examples/java_proxy");
            // System.out.println(proxDir);

            ArrayList<String> args = new ArrayList<>(5);
            args.add("./target/release/java_proxy");
            args.add(serverAddr);
            args.add("-p");
            args.add("" + port);

            if(waitForSync) {
                args.add("-y");
                args.add("" + total_clients);
            }
            final ProcessBuilder pb = new ProcessBuilder(args);
            // pb.inheritIO();
            // pb.environment().put("RUST_BACKTRACE", "short");

            pb.directory(proxDir);
            proxy = pb.start();
            bbos = new ByteBufferOutputStream();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

        try {
            Thread.sleep(100);
        } catch(InterruptedException e) {

        }
        try {
            appendSocket = new Socket("localhost", port);
            recvSocket = new Socket("localhost", port);

            // appendSocket.setTcpNoDelay(true);
            recvSocket.setTcpNoDelay(true);
        } catch(UnknownHostException e) {
            throw new RuntimeException(e);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }

        try {
            append = new DataOutputStream(new BufferedOutputStream(appendSocket.getOutputStream()));

            snapshot = new DataOutputStream(recvSocket.getOutputStream());
            recv = new DataInputStream(new BufferedInputStream(recvSocket.getInputStream()));

            append.writeInt(chains.length);
            for(int chain: chains) append.writeInt(chain);
            append.flush();

            if(waitForSync) recv.readByte();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(int chain, byte[] data) {
        append(chain, new ByteArrayData(data));
    }

    public void append(int chain, byte[] data, int length) {
        append(chain, new ByteArrayData(data, 0, length));
    }

    public <V extends ProxyHandle.Data> void append(int chain, V data) {
        try {
            append.writeInt(chain);
            DataOutputStream dos = new DataOutputStream(bbos);
            data.writeData(dos);
            bbos.writeTo(append);
            append.flush();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void append(int[] chain, byte[] data) {
        //append(chain, data, data.length);
        append(chain, new ByteArrayData(data));
    }

    public void append(int[] chain, byte[] data, int length) {
        append(chain, new ByteArrayData(data, 0, length));
    }

    public <V extends ProxyHandle.Data> void append(int[] chain, V data) {
        try {
            append.writeInt(-chain.length);
            for(int i: chain) {
                append.writeInt(i);
            }
            DataOutputStream dos = new DataOutputStream(bbos);
            data.writeData(dos);
            bbos.writeTo(append);
            append.flush();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final ProxyHandle.Bytes snapshot_and_get() {
        try {
            snapshot.write(0);
            snapshot.flush();
            return new Bytes();
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final class Bytes implements Iterable<byte[]>, Iterator<byte[]> {

        private byte[] next = null;
        private boolean done = false;

        @Override
        public boolean hasNext() {
            if(next != null) return true;
            if(done) return false;
            fetchNext();
            return next != null;
        }

        @Override
        public byte[] next() {
            if(next == null && !done) fetchNext();
            if(next == null) throw new NoSuchElementException();
            byte[] ret = next;
            next = null;
            return ret;
        }

        private void fetchNext() {
            try {
                int size = recv.readInt();
                if(size == 0) {
                    done = true;
                    next = null;
                    return;
                }
                next = new byte[size];
                recv.readFully(next, 0, size);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<byte[]> iterator() {
            return this;
        }
    }

    public final <E extends Data> ProxyHandle.DataStream<E> snapshot_and_get_data(E reader) {
        try {
            snapshot.write(0);
            snapshot.flush();
            return new DataStream(reader);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public final class DataStream<E extends Data> implements Iterable<E>, Iterator<E> {

        private final E reader;
        private E next = null;
        private boolean done = false;

        DataStream(E reader) {
            this.reader = reader;
        }

        @Override
        public boolean hasNext() {
            if(next != null) return true;
            if(done) return false;
            fetchNext();
            return next != null;
        }

        @Override
        public E next() {
            if(next == null && !done) fetchNext();
            if(next == null) throw new NoSuchElementException();
            E ret = next;
            next = null;
            return ret;
        }

        private void fetchNext() {
            try {
                int size = recv.readInt();
                if(size == 0) {
                    done = true;
                    next = null;
                    return;
                }
                next = (E)reader.readData(recv);
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Iterator<E> iterator() {
            return this;
        }
    }

    @Override
    public final void close() {
        try {
            append.close();
            snapshot.close();
            recv.close();
            appendSocket.close();
            recvSocket.close();
            proxy.destroy();
        } catch(Exception e) {

        }
    }

    public static interface Data {
        public void writeData(DataOutputStream out) throws IOException;

        public Data readData(DataInputStream in)  throws IOException;

        default public void readFields(DataInputStream in)  throws IOException {
            readData(in);
        }
    }

    public static class ByteArrayData implements Data {
        private final byte[] data;
        private final int start;
        private final int length;

        public ByteArrayData(byte[] data) {
            this.data = data;
            this.start = 0;
            this.length = data.length;
        }

        public ByteArrayData(byte[] data, int start, int length) {
            this.data = data;
            this.start = start;
            this.length = length;
        }

        @Override
        public final void writeData(DataOutputStream out) throws IOException {
            out.writeInt(length);
            out.write(data, start, length);
        }

        @Override
        public final ByteArrayData readData(DataInputStream in) throws IOException {
            int length = in.readInt();
            byte[] array = new byte[length];
            in.readFully(array);
            return new ByteArrayData(array);
        }
    }


    private final static class ByteBufferOutputStream extends java.io.OutputStream {
		private ByteBuffer buffer = ByteBuffer.allocate(512);

		public final void write(byte[] b) {
			write(ByteBuffer.wrap(b));
		}

		public final void write(byte[] b, int off, int len) {
			write(ByteBuffer.wrap(b, off, len));
		}

		public final void write(ByteBuffer b) {
			if(buffer.remaining() < b.remaining()) {
				int newCap = buffer.capacity() * 2;
				if (newCap - buffer.remaining() < b.remaining()) {
					newCap = buffer.capacity() + b.remaining();
				}
				ByteBuffer newBuffer = ByteBuffer.allocate(newCap);
				this.buffer.flip();
				newBuffer.put(this.buffer);
				this.buffer = newBuffer;
			}

			this.buffer.put(b);
		}

		public final void write(int b) {
			this.buffer.put((byte) b);
        }

        public final void writeTo(DataOutputStream os) throws IOException {
            this.buffer.flip();
            int len = this.buffer.remaining();
            os.writeInt(len);
            os.write(this.buffer.array(), 0, len);
            this.buffer.clear();
        }
	}
}