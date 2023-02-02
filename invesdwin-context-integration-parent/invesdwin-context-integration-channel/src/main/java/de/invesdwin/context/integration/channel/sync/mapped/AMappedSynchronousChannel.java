package de.invesdwin.context.integration.channel.sync.mapped;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.MemoryMappedFile;
import de.invesdwin.util.streams.buffer.bytes.extend.UnsafeByteBuffer;

@NotThreadSafe
public abstract class AMappedSynchronousChannel implements ISynchronousChannel {

    public static final byte TRANSACTION_INITIAL_VALUE = 0;
    public static final byte TRANSACTION_WRITING_VALUE = -1;
    public static final byte TRANSACTION_CLOSED_VALUE = -2;

    public static final int TRANSACTION_INDEX = 0;
    public static final int TRANSACTION_SIZE = Byte.BYTES;

    public static final int READFINISHED_INDEX = TRANSACTION_INDEX + TRANSACTION_SIZE;
    public static final int READFINISHED_SIZE = Byte.BYTES;

    public static final byte READFINISHED_FALSE = 0;
    public static final byte READFINISHED_TRUE = 1;
    public static final byte READFINISHED_CLOSED = -1;

    public static final int SIZE_INDEX = READFINISHED_INDEX + READFINISHED_SIZE;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    public static final int MIN_PHYSICAL_MESSAGE_SIZE = 4096 - MESSAGE_INDEX;

    protected MemoryMappedFile mem;
    protected UnsafeByteBuffer buffer;
    protected final File file;
    private final int maxMessageSize;

    public AMappedSynchronousChannel(final File file, final int maxMessageSize) {
        this.file = file;
        if (maxMessageSize <= 0) {
            throw new IllegalArgumentException("fileSize needs to be positive");
        }
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        final int fileSize = maxMessageSize + MESSAGE_INDEX;
        try {
            this.mem = new MemoryMappedFile(file.getAbsolutePath(), fileSize, false);
            this.buffer = new UnsafeByteBuffer(mem.getAddress(), Integers.checkedCast(mem.getLength()));
        } catch (final Exception e) {
            throw new IOException("Unable to open file: " + file, e);
        }
    }

    protected byte getNextTransaction() {
        byte transaction = getTransaction();
        if (transaction == TRANSACTION_WRITING_VALUE) {
            throw new IllegalStateException(
                    "Someone else seems is writing a transaction, exclusive file access is needed!");
        }
        do {
            transaction++;
        } while (transaction == TRANSACTION_WRITING_VALUE || transaction == TRANSACTION_CLOSED_VALUE
                || transaction == TRANSACTION_INITIAL_VALUE);
        return transaction;
    }

    protected void setTransaction(final byte val) {
        buffer.putByteVolatile(TRANSACTION_INDEX, val);
    }

    protected byte getTransaction() {
        return buffer.getByteVolatile(TRANSACTION_INDEX);
    }

    protected byte getReadFinished() {
        return buffer.getByteVolatile(READFINISHED_INDEX);
    }

    protected boolean isReadFinished() throws IOException {
        final byte readFinished = getReadFinished();
        if (readFinished == READFINISHED_CLOSED) {
            throw FastEOFException.getInstance("Channel was closed by the other endpoint");
        }
        return readFinished == READFINISHED_TRUE;
    }

    protected void setReadFinished(final byte val) {
        buffer.putByteVolatile(READFINISHED_INDEX, val);
    }

    protected void setSize(final int val) {
        if (val > maxMessageSize) {
            throw new IllegalStateException(
                    "messageSize [" + val + "] exceeds maxMessageSize [" + maxMessageSize + "]");
        }
        buffer.putInt(SIZE_INDEX, val);
    }

    protected int getSize() {
        return buffer.getInt(SIZE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (mem != null) {
            try {
                mem.close();
                mem = null;
            } catch (final Exception e) {
                throw new IOException("Unable to close the file: " + file, e);
            }
        }
    }

}
