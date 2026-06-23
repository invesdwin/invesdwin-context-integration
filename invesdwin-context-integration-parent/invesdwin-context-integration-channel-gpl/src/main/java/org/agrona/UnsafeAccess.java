package org.agrona;

import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

/**
 * HardroNio requires UnsafeAccess to be restored from older agrona versions (1.x.x)
 */
@Immutable
public final class UnsafeAccess {
    public static final sun.misc.Unsafe UNSAFE;
    public static final int ARRAY_BYTE_BASE_OFFSET;
    public static final boolean MEMSET_HACK_REQUIRED;
    public static final int MEMSET_HACK_THRESHOLD;

    static {
        sun.misc.Unsafe unsafe = null;
        try {
            unsafe = sun.misc.Unsafe.getUnsafe();
        } catch (final Exception ex) {
            try {
                final Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                f.setAccessible(true);

                unsafe = (sun.misc.Unsafe) f.get(null);
            } catch (final Exception ex2) {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        UNSAFE = unsafe;
        ARRAY_BYTE_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

        boolean memsetHackRequired;
        try {
            Class.forName("java.lang.Runtime$Version"); // since JDK 9
            memsetHackRequired = false;
        } catch (final ClassNotFoundException ex) {
            memsetHackRequired = true;
        }
        MEMSET_HACK_REQUIRED = memsetHackRequired;
        MEMSET_HACK_THRESHOLD = 64;
    }

    private UnsafeAccess() {}
}