package test;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.*;
import com.sun.jna.ptr.PointerByReference;
import c_link.*;

import fuzzy_log.*;
import fuzzy_log.FuzzyLog.ReadHandleAndWriteHandle;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

public class Test {
    public static void main(String... args) {
        System.out.println("Test Start.");
        System.out.println("JNA library path:");
        System.out.println("\t" + System.getProperty("jna.library.path"));
        System.out.println("Java library path:");
        System.out.println("\t" + System.getProperty("java.library.path"));
        System.out.println();
        try {
            //System.in.read();
            /*byte[] d = new byte[461];
            for(int i = 0; i < d.length; i++) d[i] = (byte)i;

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            DataOutputStream test = new DataOutputStream(baos);
            // baos.write(d);
            test.writeInt(d.length);
            test.write(d);
            test.flush();
            byte[] b = baos.toByteArray();
            System.out.println(b.length);
            System.out.print("[");
            for(int i = 0; i < b.length; i++) {
                System.out.print("" + b[i] + ", ");
            }
            System.out.println("]");*/
        } catch(Exception e) {}
        // System.exit(0);


        byte[] ipString;
        try {
            ipString = "127.0.0.1:13890\0".getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("no utf-8");
        }
        // FuzzyLogLibrary.start_fuzzy_log_server_thread("127.0.0.1:13890\0");
        Thread.yield();
        Thread.yield();


        /*try {
            Thread.sleep(1);
        } catch (Exception e) {}
        colors.ByReference interesting = Utils.new_colors(new int[] {2, 3, 4});
        Pointer handle = FuzzyLogLibrary.new_dag_handle_with_skeens(new size_t(1), new StringArray(new String[] {"127.0.0.1:13890\0"}), interesting);
        Memory data = new Memory(64);
        colors.ByReference colors = new colors.ByReference();
        {
            data.setInt(0, 401);
            Utils.set_colors(colors, new int[] {4});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        {
            data.setInt(0, 102);
            Utils.set_colors(colors, new int[] {2});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        {
            data.setInt(0, 733);
            Utils.set_colors(colors, new int[] {3});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        int end_key = ThreadLocalRandom.current().nextInt();
        {
            data.setInt(0, end_key);
            Utils.set_colors(colors, new int[] {2, 3, 4});
            //System.out.println("sending" + data + " to all");
            FuzzyLogLibrary.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }

        FuzzyLogLibrary.snapshot(handle);

        size_tByReference num_colors = new size_tByReference();
        while(num_colors.getValue() != 0) {
            size_tByReference data_size = new size_tByReference();
            c_link.get_next_val.ByValue next_val = FuzzyLogLibrary.get_next2(handle, data_size, num_colors);
            if(num_colors.getValue() != 0) {
                System.out.print("read " + data_size.getValue() + " bytes = " + next_val.data.getInt(0) + " from ");
                fuzzy_log_location[] locs = (fuzzy_log_location[])next_val.locs.toArray((int)num_colors.getValue());
                for(fuzzy_log_location loc: locs) {
                    System.out.print(" (color: " + loc.color + " entry: " + loc.entry + ")");
                }
                System.out.println();
            } else {
                System.out.println("finished reading.");
            }
        }
        FuzzyLogLibrary.close_dag_handle(handle);*/

/*
        System.out.println("HL test.");

        FuzzyLog log = new FuzzyLog(new String[] {"127.0.0.1:13890\0"}, new int[] {5, 6, 7});
        {
            log.async_append(new int[]{6}, new byte[] {(byte)2, (byte)3, (byte)1});
            log.wait_any_append();
        }
        {
            log.async_append(new int[]{5}, new byte[] {(byte)122});
            log.wait_any_append();
        }
        {
            log.async_append(new int[]{7}, new byte[] {(byte)3, (byte)3, (byte)3, (byte)3, (byte)3, (byte)3});
            log.wait_any_append();
        }
        {
            byte[] end_data = new byte[4];
            for(int i = 0; i < end_data.length; i++) end_data[i] = (byte)ThreadLocalRandom.current().nextInt();
            log.async_append(new int[] {5, 6, 7}, end_data);
            log.wait_any_append();
        }

        log.snapshot();

        FuzzyLog.Events events = log.get_events();
        for(FuzzyLog.Event event: events) {
            System.out.println("read: " + Arrays.toString(event.data) + " locs: " + Arrays.toString(event.locations));
        }
*/

        //System.out.println("split test");

        // ReadHandleAndWriteHandle raw = new FuzzyLog(new String[] {"127.0.0.1:13890\0"}, new int[] {8, 9, 10}).split();
        /*{
            raw.writer.async_append(new int[]{9}, new byte[] {(byte)2, (byte)3, (byte)1});
            raw.writer.wait_any_append();
        }
        {
            raw.writer.async_append(new int[]{8}, new byte[] {(byte)122});
            raw.writer.wait_any_append();
        }
        {
            raw.writer.async_append(new int[]{10}, new byte[] {(byte)3, (byte)3, (byte)3, (byte)3, (byte)3, (byte)3});
            raw.writer.wait_any_append();
        }
        {
            byte[] end_data = new byte[4];
            for(int i = 0; i < end_data.length; i++) end_data[i] = (byte)ThreadLocalRandom.current().nextInt();
            raw.writer.async_append(new int[] {8, 9, 10}, end_data);
            raw.writer.wait_any_append();
        }

        raw.reader.snapshot();

        ReadHandle.Events revents = raw.reader.get_events();
        for(ReadHandle.Event event: revents) {
            System.out.println("read: " + Arrays.toString(event.data) + " locs: " + Arrays.toString(event.locations));
        }*/

        try {
            long total = 0;
            SerTest testObject = new SerTest("/abcd/1", "AAA".getBytes(), null, SerTest.CreateMode.PERSISTENT);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for(int i = 0; i < 100_000; i++) {
                long start = System.nanoTime();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(testObject);
                oos.close();
                byte[] bytes = baos.toByteArray();
                ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bios);
                Object o = ois.readObject();
                long elapsed = System.nanoTime() - start;
                total += elapsed;
                assert(o instanceof SerTest);
                baos.reset();
            }
            System.out.println("serde " + 100_000 + " in " + total + " ns, " + (1_000_000_000.0 * ((double)100_000) / (double)total) + " Hz");
        }
        catch(IOException | ClassNotFoundException e) { throw new RuntimeException(e); }

        try {
            long total = 0;
            SerTest testObject = new SerTest("/abcd/1", "AAA".getBytes(), null, SerTest.CreateMode.PERSISTENT);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            for(int i = 0; i < 100_000; i++) {
                long start = System.nanoTime();
                DataOutputStream daos = new DataOutputStream(baos);
                testObject.writeData(daos);
                daos.close();
                byte[] bytes = baos.toByteArray();
                ByteArrayInputStream bios = new ByteArrayInputStream(bytes);
                DataInputStream dis = new DataInputStream(bios);
                SerTest o = SerTest.readData(dis);
                long elapsed = System.nanoTime() - start;
                total += elapsed;
                assert(o instanceof SerTest);
                baos.reset();
            }
            System.out.println("data " + 100_000 + " in " + total + " ns, " + (1_000_000_000.0 * ((double)100_000) / (double)total) + " Hz");
        }
        catch(IOException e) { throw new RuntimeException(e); }

        final int numAppends = 100_000;
        {
            System.out.println("Direct");

            ReadHandleAndWriteHandle raw = new FuzzyLog(new String[] {"127.0.0.1:13890\0"}, new int[] {53}).split();
            long start = System.nanoTime();
            Thread writer = new Thread(() -> {
                byte[] data = new byte[400];
                for(int i = 0; i < numAppends; i ++) {
                    raw.writer.async_append(53, data);
                    raw.writer.flush_completed_appends();
                }
            });
            writer.start();
            Thread.yield();
            int gotten = 0;
            while(gotten < numAppends) {
                ReadHandle.Buffers buffers = raw.reader.snapshot_and_get_buffer();
                for (ByteBuffer b: buffers) gotten += 1;
            }

            try {
                writer.join();
            } catch(InterruptedException e) {}


            long elapsed = System.nanoTime() - start;
            System.out.println("got " + gotten + " in " + elapsed + " ns, " + (1_000_000_000.0 * ((double)gotten) / (double)elapsed) + " Hz");
        }

        /*{
            System.out.println("Proxy");

            try {
                Socket append = new Socket("localhost", 13336);
                DataOutputStream out = new DataOutputStream(new BufferedOutputStream( append.getOutputStream()));
                out.writeInt(1);
                out.writeInt(1);
                out.flush();

                Socket recv = new Socket("localhost", 13336);

                long start = System.nanoTime();
                Thread writer = new Thread(() -> {
                    try {
                        byte[] data = new byte[461];
                        for(int i = 0; i < numAppends; i ++) {
                            out.writeInt(1);
                            // out.writeInt(1);
                            out.writeInt(data.length);
                            out.write(data);
                            out.flush();
                        }
                    } catch(IOException e) {
                        throw new RuntimeException(e);
                    }

                });
                writer.start();
                Thread.yield();




                DataOutputStream recvSignal = new DataOutputStream(recv.getOutputStream());
                DataInputStream in = new DataInputStream(new BufferedInputStream(recv.getInputStream()));
                int gotten = 0;
                while(gotten < numAppends) {
                    recvSignal.write(0);
                    recvSignal.flush();
                    byte[] input = new byte[512];
                    for(int size = in.readInt(); size != 0; size = in.readInt()) {
                        gotten += 1;
                        if(size > input.length) {
                            input = new byte[size];
                        }
                        in.readFully(input, 0, size);
                    }
                }

                try {
                    writer.join();
                } catch(InterruptedException e) {}
                long elapsed = System.nanoTime() - start;
                System.out.println("got " + gotten + " in " + elapsed + " ns, " + (1_000_000_000.0 * ((double)gotten) / (double)elapsed) + " Hz");
            } catch(IOException e) {
                throw new RuntimeException(e);
            }
        }*/

        {
            System.out.println("ProxyHandle");

            try(ProxyHandle h = new ProxyHandle("172.31.5.104:13289", 13349, 1l, 7)) {

                long start = System.nanoTime();
                Thread writer = new Thread(() -> {
                    byte[] data = new byte[461];
                    for(int i = 0; i < numAppends; i ++) {
                        h.append(7, data);
                    }
                });
                writer.start();
                Thread.yield();

                int gotten = 0;
                while(gotten < numAppends) {
                    ProxyHandle.Bytes events = h.snapshot_and_get();
                    for(byte[] event: events) gotten += 1;
                }

                try {
                    writer.join();
                } catch(InterruptedException e) {}
                long elapsed = System.nanoTime() - start;
                System.out.println("got " + gotten + " in " + elapsed + " ns, " + (1_000_000_000.0 * ((double)gotten) / (double)elapsed) + " Hz");
            }

            System.out.println("Done.");
        }
    }
}

class HasId implements Serializable {
    int Id;

    static AtomicInteger idcounter = new AtomicInteger();
	int id;
	public HasId()
	{
		//FIXME distringuish clients
		id = idcounter.getAndIncrement();
		//System.out.println(this + "::" + this.id);
	}
}

final class SerTest extends HasId implements Serializable {
    static enum CreateMode implements Serializable {
        PERSISTENT (0, false, false, false, false),
        PERSISTENT_SEQUENTIAL (2, false, true, false, false),
        EPHEMERAL (1, true, false, false, false),
        EPHEMERAL_SEQUENTIAL (3, true, true, false, false),
        CONTAINER (4, false, false, true, false),
        PERSISTENT_WITH_TTL(5, false, false, false, true),
        PERSISTENT_SEQUENTIAL_WITH_TTL(6, false, true, false, true);

        private boolean ephemeral;
        private boolean sequential;
        private final boolean isContainer;
        private int flag;
        private boolean isTTL;

        CreateMode(int flag, boolean ephemeral, boolean sequential,
                   boolean isContainer, boolean isTTL) {
            this.flag = flag;
            this.ephemeral = ephemeral;
            this.sequential = sequential;
            this.isContainer = isContainer;
            this.isTTL = isTTL;
        }

        public boolean isEphemeral() {
            return ephemeral;
        }

        public boolean isSequential() {
            return sequential;
        }

        public boolean isContainer() {
            return isContainer;
        }

        public boolean isTTL() {
            return isTTL;
        }

        public int toFlag() {
            return flag;
        }

        /**
         * Map an integer value to a CreateMode value
         */
        static public CreateMode fromFlag(int flag) {
            switch(flag) {
            case 0: return CreateMode.PERSISTENT;

            case 1: return CreateMode.EPHEMERAL;

            case 2: return CreateMode.PERSISTENT_SEQUENTIAL;

            case 3: return CreateMode.EPHEMERAL_SEQUENTIAL ;

            case 4: return CreateMode.CONTAINER;

            case 5: return CreateMode.PERSISTENT_WITH_TTL;

            case 6: return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

            default:
                throw new RuntimeException();
            }
        }

        /**
         * Map an integer value to a CreateMode value
         */
        static public CreateMode fromFlag(int flag, CreateMode defaultMode) {
            switch(flag) {
                case 0:
                    return CreateMode.PERSISTENT;

                case 1:
                    return CreateMode.EPHEMERAL;

                case 2:
                    return CreateMode.PERSISTENT_SEQUENTIAL;

                case 3:
                    return CreateMode.EPHEMERAL_SEQUENTIAL;

                case 4:
                    return CreateMode.CONTAINER;

                case 5:
                    return CreateMode.PERSISTENT_WITH_TTL;

                case 6:
                    return CreateMode.PERSISTENT_SEQUENTIAL_WITH_TTL;

                default:
                    return defaultMode;
            }
        }
    }

    static class ACL implements Serializable {
        private int p;
        private Id id;

        public void write(DataOutput out) throws IOException {
            out.writeInt(p);
            id.write(out);
        }

        public void readFields(DataInput in) throws IOException {
            p = in.readInt();
            String scheme = in.readUTF();
            String id = in.readUTF();
            this.id = new Id(scheme, id);
        }
    }

    static class Id implements Serializable {
        private String scheme;
        private String id;

        public Id(String scheme, String id) {
            this.scheme = scheme;
            this.id = id;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(scheme);
            out.writeUTF(id);
        }
    }

    CreateMode cm;
    byte[] data;
    List<ACL> acl;
    String path;

    public SerTest(String path, byte[] data, List<ACL> acl, CreateMode createMode)
    {
        this.path = path;
        this.data = data;
        this.acl = acl;
        this.cm = createMode;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException
    {
        out.writeObject(cm);
        out.writeObject(data);
        out.writeObject(acl);
        out.writeObject(path);
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        cm = (CreateMode)in.readObject();
        data = (byte[])in.readObject();
        acl = (List<ACL>)in.readObject();
        path = (String)in.readObject();
    }

    public void writeData(DataOutputStream out) throws IOException {
        out.writeInt(cm.toFlag());
        out.writeInt(data.length);
        out.write(data);
        if(acl != null) {
            int len = acl.size();
            out.writeInt(len);
            for(ACL a: acl) a.write(out);
        } else {
            out.writeInt(0);
        }
        out.writeUTF(path);
    }

    public static SerTest readData(DataInputStream in) throws IOException {
        CreateMode createMode = CreateMode.fromFlag(in.readInt());
        int dataLen = in.readInt();
        byte[] data = new byte[dataLen];
        in.read(data);
        int aclLen = in.readInt();
        //TODO constant empty immutable List
        ArrayList<ACL> acl = new ArrayList<>(aclLen);
        for(int i = 0; i < aclLen; i++) {
            ACL a = new ACL();
            a.readFields(in);
            acl.add(a);
        }
        String path = in.readUTF();
        return new SerTest(path, data, acl, createMode);
    }
}
