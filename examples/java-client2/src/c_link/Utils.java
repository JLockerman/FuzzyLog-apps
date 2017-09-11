package c_link;

import c_link.*;

import com.sun.jna.Memory;
import com.sun.jna.Native;

public class Utils {
    public static colors.ByReference new_colors(int[] new_colors) {
        colors.ByReference colors = new colors.ByReference();
        Utils.set_colors(colors, new_colors);
        return colors;
    }

    public static void set_colors(colors.ByReference colors, int[] new_colors) {
        colors.mycolors = new Memory(new_colors.length * Native.getNativeSize(Integer.TYPE));
        colors.mycolors.write(0, new_colors, 0, new_colors.length);
        colors.numcolors = new size_t(new_colors.length);
    }
}
