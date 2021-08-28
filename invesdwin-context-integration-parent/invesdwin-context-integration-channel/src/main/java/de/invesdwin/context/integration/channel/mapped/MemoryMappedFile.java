package de.invesdwin.context.integration.channel.mapped;

import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.IoUtil;

/**
 * Class for direct access to a memory mapped file.
 * 
 * This class was inspired from an entry in Bryce Nyeggen's blog
 *
 * https://github.com/caplogic/Mappedbus/blob/master/src/main/io/mappedbus/MemoryMappedFile.java
 *
 */
@NotThreadSafe
public class MemoryMappedFile {

    private final long address;
    private final int size;
    private final String loc;

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
    public MemoryMappedFile(final String loc, final int len) throws Exception {
        this.loc = loc;
        this.size = roundTo4096(len);
        this.address = mapAndSetOffset();
    }

    public long getAddress() {
        return address;
    }

    public int getSize() {
        return size;
    }

    private static int roundTo4096(final int i) {
        return (i + 0xfff) & ~0xfff;
    }

    private long mapAndSetOffset() throws Exception {
        final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
        backingFile.setLength(this.size);
        final FileChannel ch = backingFile.getChannel();
        final long address = IoUtil.map(ch, MapMode.READ_WRITE, 0L, this.size);
        ch.close();
        backingFile.close();
        return address;
    }

    public void unmap() throws Exception {
        IoUtil.unmap(null, address, this.size);
    }

}