package de.invesdwin.context.integration.channel.mapped;

import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.channels.FileChannel;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.reflection.Reflections;

/**
 * Class for direct access to a memory mapped file.
 * 
 * This class was inspired from an entry in Bryce Nyeggen's blog
 *
 * https://github.com/caplogic/Mappedbus/blob/master/src/main/io/mappedbus/MemoryMappedFile.java
 *
 */
@SuppressWarnings("restriction")
@NotThreadSafe
public class MemoryMappedFile {

    private static final sun.misc.Unsafe UNSAFE = DynamicInstrumentationReflections.getUnsafe();
    private static final IMap0Invoker MMAP;
    private static final Method UNMMAP;
    private static final int BYTE_ARRAY_OFFSET;

    private final long address;
    private final long size;
    private final String loc;

    static {
        try {
            final Class<Object> fileChannelImplClass = Reflections.classForName("sun.nio.ch.FileChannelImpl");
            MMAP = getMmap(fileChannelImplClass);
            UNMMAP = getMethod(fileChannelImplClass, "unmap0", long.class, long.class);
            BYTE_ARRAY_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Constructs a new memory mapped file.
     * 
     * @param loc
     *            the file name
     * @param len
     *            the file length
     * @throws Exception
     *             in case there was an error creating the memory mapped file
     */
    public MemoryMappedFile(final String loc, final long len) throws Exception {
        this.loc = loc;
        this.size = roundTo4096(len);
        this.address = mapAndSetOffset();
    }

    public long getAddress() {
        return address;
    }

    public long getSize() {
        return size;
    }

    @FunctionalInterface
    private interface IMap0Invoker {
        long map0(FileChannel channel, int prot, long position, long length) throws Exception;
    }

    private static IMap0Invoker getMmap(final Class<Object> fileChannelImplClass) throws Exception {
        try {
            //java < 14
            final Method method = getMethod(fileChannelImplClass, "map0", int.class, long.class, long.class);
            return (channel, prot, position, length) -> (long) method.invoke(channel, prot, position, length);
        } catch (final Throwable t) {
            //java >= 14
            final Method method = getMethod(fileChannelImplClass, "map0", int.class, long.class, long.class,
                    boolean.class);
            //https://github.com/OpenHFT/Chronicle-Core/blob/ea/src/main/java/net/openhft/chronicle/core/OS.java they also use sync=false
            final boolean isSync = false;
            return (channel, prot, position, length) -> (long) method.invoke(channel, prot, position, length, isSync);
        }
    }

    private static Method getMethod(final Class<?> cls, final String name, final Class<?>... params) throws Exception {
        final Method m = cls.getDeclaredMethod(name, params);
        m.setAccessible(true);
        return m;
    }

    private static long roundTo4096(final long i) {
        return (i + 0xfffL) & ~0xfffL;
    }

    private long mapAndSetOffset() throws Exception {
        final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
        backingFile.setLength(this.size);
        final FileChannel ch = backingFile.getChannel();
        final long address = MMAP.map0(ch, 1, 0L, this.size);
        ch.close();
        backingFile.close();
        return address;
    }

    public void unmap() throws Exception {
        UNMMAP.invoke(null, address, this.size);
    }

    /**
     * Reads a byte from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public byte getByte(final long pos) {
        return UNSAFE.getByte(pos + address);
    }

    /**
     * Reads a byte (volatile) from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public byte getByteVolatile(final long pos) {
        return UNSAFE.getByteVolatile(null, pos + address);
    }

    /**
     * Reads an int from the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @return the value read
     */
    public int getInt(final long pos) {
        return UNSAFE.getInt(pos + address);
    }

    /**
     * Reads an int (volatile) from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public int getIntVolatile(final long pos) {
        return UNSAFE.getIntVolatile(null, pos + address);
    }

    /**
     * Reads a long from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public long getLong(final long pos) {
        return UNSAFE.getLong(pos + address);
    }

    /**
     * Reads a long (volatile) from the specified position.
     * 
     * @param pos
     *            position in the memory mapped file
     * @return the value read
     */
    public long getLongVolatile(final long pos) {
        return UNSAFE.getLongVolatile(null, pos + address);
    }

    /**
     * Writes a byte to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putByte(final long pos, final byte val) {
        UNSAFE.putByte(pos + address, val);
    }

    /**
     * Writes a byte (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putByteVolatile(final long pos, final byte val) {
        UNSAFE.putByteVolatile(null, pos + address, val);
    }

    /**
     * Writes an int to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putInt(final long pos, final int val) {
        UNSAFE.putInt(pos + address, val);
    }

    /**
     * Writes an int (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putIntVolatile(final long pos, final int val) {
        UNSAFE.putIntVolatile(null, pos + address, val);
    }

    /**
     * Writes a long to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putLong(final long pos, final long val) {
        UNSAFE.putLong(pos + address, val);
    }

    /**
     * Writes a long (volatile) to the specified position.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param val
     *            the value to write
     */
    public void putLongVolatile(final long pos, final long val) {
        UNSAFE.putLongVolatile(null, pos + address, val);
    }

    /**
     * Reads a buffer of data.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param data
     *            the input buffer
     * @param offset
     *            the offset in the buffer of the first byte to read data into
     * @param length
     *            the length of the data
     */
    public void getBytes(final long pos, final byte[] data, final int offset, final int length) {
        UNSAFE.copyMemory(null, pos + address, data, BYTE_ARRAY_OFFSET + offset, length);
    }

    /**
     * Writes a buffer of data.
     * 
     * @param pos
     *            the position in the memory mapped file
     * @param data
     *            the output buffer
     * @param offset
     *            the offset in the buffer of the first byte to write
     * @param length
     *            the length of the data
     */
    public void setBytes(final long pos, final byte[] data, final int offset, final int length) {
        UNSAFE.copyMemory(data, BYTE_ARRAY_OFFSET + offset, null, pos + address, length);
    }

    public boolean compareAndSwapInt(final long pos, final int expected, final int value) {
        return UNSAFE.compareAndSwapInt(null, pos + address, expected, value);
    }

    public boolean compareAndSwapLong(final long pos, final long expected, final long value) {
        return UNSAFE.compareAndSwapLong(null, pos + address, expected, value);
    }

    public long getAndAddLong(final long pos, final long delta) {
        return UNSAFE.getAndAddLong(null, pos + address, delta);
    }
}