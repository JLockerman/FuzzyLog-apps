package test;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.*;
import com.sun.jna.ptr.PointerByReference;
import c_link.*;

import java.nio.ByteBuffer;

import java.util.concurrent.ThreadLocalRandom;

public class Test {
    public static void main(String... args) {
        System.out.println("Test Start.");
        System.out.println("JNA library path:");
        System.out.println("\t" + System.getProperty("jna.library.path"));
        System.out.println("Java library path:");
        System.out.println("\t" + System.getProperty("java.library.path"));
        byte[] ipString;
        try {
            ipString = "127.0.0.1:13890\0".getBytes("UTF-8");
        } catch (Exception e) {
            throw new RuntimeException("no utf-8");
        }
        FuzzyLogLibrary.INSTANCE.start_fuzzy_log_server_thread("127.0.0.1:13890\0");
        try {
            Thread.sleep(1);
        } catch (Exception e) {}
        colors.ByReference interesting = Utils.new_colors(new int[] {2, 3, 4});
        Pointer handle = FuzzyLogLibrary.INSTANCE.new_dag_handle_with_skeens(new size_t(1), new String[] {"127.0.0.1:13890\0"}, interesting);
        Memory data = new Memory(64);
        colors.ByReference colors = new colors.ByReference();
        {
            data.setInt(0, 401);
            Utils.set_colors(colors, new int[] {4});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.INSTANCE.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        {
            data.setInt(0, 102);
            Utils.set_colors(colors, new int[] {2});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.INSTANCE.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        {
            data.setInt(0, 733);
            Utils.set_colors(colors, new int[] {3});
            //System.out.println("sending" + data + " to " + colors);
            FuzzyLogLibrary.INSTANCE.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }
        int end_key = ThreadLocalRandom.current().nextInt();
        {
            data.setInt(0, end_key);
            Utils.set_colors(colors, new int[] {2, 3, 4});
            //System.out.println("sending" + data + " to all");
            FuzzyLogLibrary.INSTANCE.do_append(handle, data, new size_t(4), colors, null, (byte)0);
        }

        FuzzyLogLibrary.INSTANCE.snapshot(handle);

        size_tByReference num_colors = new size_tByReference();
        while(num_colors.getValue() != 0) {
            size_tByReference data_size = new size_tByReference();
            c_link.get_next_val.ByValue next_val = FuzzyLogLibrary.INSTANCE.get_next2(handle, data_size, num_colors);
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
        FuzzyLogLibrary.INSTANCE.close_dag_handle(handle);

        System.out.println("Done.");
    }
}