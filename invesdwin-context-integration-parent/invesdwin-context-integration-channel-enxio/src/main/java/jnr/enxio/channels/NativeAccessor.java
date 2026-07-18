package jnr.enxio.channels;

import javax.annotation.concurrent.Immutable;

import jnr.enxio.channels.Native.LibC;

@Immutable
public final class NativeAccessor {

    private NativeAccessor() {}

    public static LibC libc() {
        return Native.libc();
    }

}
