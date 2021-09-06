package de.invesdwin.context.integration.channel.sync.mapped;

import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.IoUtil;

import de.invesdwin.util.lang.finalizer.AFinalizer;

/**
 * Class for direct access to a memory mapped file.
 * 
 * This class was inspired from an entry in Bryce Nyeggen's blog
 *
 * https://github.com/caplogic/Mappedbus/blob/master/src/main/io/mappedbus/MemoryMappedFile.java
 *
 */
@NotThreadSafe
public class MemoryMappedFile implements Closeable {

    private final MemoryMappedFileFinalizer finalizer;

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
    public MemoryMappedFile(final String loc, final int len) throws IOException {
        this.finalizer = new MemoryMappedFileFinalizer(loc, len);
        this.finalizer.register(this);
    }

    public long getAddress() {
        return finalizer.address;
    }

    public int getSize() {
        return finalizer.size;
    }

    public boolean isClosed() {
        return finalizer.isClosed();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class MemoryMappedFileFinalizer extends AFinalizer {
        private final String loc;
        private final long address;
        private final int size;
        private boolean cleaned;

        private MemoryMappedFileFinalizer(final String loc, final int len) throws IOException {
            this.loc = loc;
            this.size = roundTo4096(len);
            this.address = mapAndSetOffset();
        }

        @Override
        protected void clean() {
            IoUtil.unmap(null, address, this.size);
            cleaned = true;
        }

        @Override
        protected boolean isCleaned() {
            return cleaned;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

        private static int roundTo4096(final int i) {
            return (i + 0xfff) & ~0xfff;
        }

        private long mapAndSetOffset() throws IOException {
            final RandomAccessFile backingFile = new RandomAccessFile(this.loc, "rw");
            backingFile.setLength(this.size);
            final FileChannel ch = backingFile.getChannel();
            final long address = IoUtil.map(ch, MapMode.READ_WRITE, 0L, this.size);
            ch.close();
            backingFile.close();
            return address;
        }
    }

}