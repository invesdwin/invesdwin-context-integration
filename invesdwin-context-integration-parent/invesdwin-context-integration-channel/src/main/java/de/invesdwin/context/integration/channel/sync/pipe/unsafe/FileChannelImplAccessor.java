package de.invesdwin.context.integration.channel.sync.pipe.unsafe;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public final class FileChannelImplAccessor {

    private static final IWrite0Provider WRITE0_PROVIDER;
    private static final MethodHandle READ0_MH;

    static {
        try {
            final Class<?> fdi = Class.forName("sun.nio.ch.FileDispatcherImpl");
            WRITE0_PROVIDER = newWrite0Provider(fdi);

            final Method read0 = Reflections.findMethod(fdi, "read0", FileDescriptor.class, long.class, int.class);
            Reflections.makeAccessible(read0);
            READ0_MH = MethodHandles.lookup().unreflect(read0);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private FileChannelImplAccessor() {}

    public static int write0(final FileDescriptor fd, final long address, final int len) throws IOException {
        return WRITE0_PROVIDER.write0(fd, address, len);
    }

    public static int read0(final FileDescriptor fd, final long address, final int len) throws IOException {
        try {
            return (int) READ0_MH.invokeExact(fd, address, len);
        } catch (final IOException ioe) {
            throw ioe;
        } catch (final Throwable e) {
            throw new IOException(e);
        }
    }

    private static IWrite0Provider newWrite0Provider(final Class<?> fdi) throws IllegalAccessException {
        try {
            final Method write0 = Reflections.findMethod(fdi, "write0", FileDescriptor.class, long.class, int.class);
            Reflections.makeAccessible(write0);
            final MethodHandle write0Mh = MethodHandles.lookup().unreflect(write0);
            return (fd, address, len) -> {
                try {
                    return (int) write0Mh.invokeExact(fd, address, len);
                } catch (final IOException ioe) {
                    throw ioe;
                } catch (final Throwable e) {
                    throw new IOException(e);
                }
            };
        } catch (final Throwable ae) {
            final Method write0 = Reflections.findMethod(fdi, "write0", FileDescriptor.class, long.class, int.class,
                    boolean.class);
            Reflections.makeAccessible(write0);
            final MethodHandle write0Mh2 = MethodHandles.lookup().unreflect(write0);
            return (fd, address, len) -> {
                try {
                    return (int) write0Mh2.invokeExact(fd, address, len);
                } catch (final IOException ioe) {
                    throw ioe;
                } catch (final Throwable e) {
                    throw new IOException(e);
                }
            };
        }
    }

    private interface IWrite0Provider {
        int write0(FileDescriptor fd, long address, int len) throws IOException;
    }
}
