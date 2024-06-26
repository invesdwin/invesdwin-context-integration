package de.invesdwin.context.integration.channel.sync.mapped.blocking;

import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.AMappedSynchronousChannel;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

/**
 * There can be multiple readers per file, but it is better to only have one.
 * 
 * Excerpt from SynchronousQueue: Blocking is mainly accomplished using LockSupport park/unpark, except that nodes that
 * appear to be the next ones to become fulfilled first spin a bit (on multiprocessors only). On very busy synchronous
 * queues, spinning can dramatically improve throughput. And on less busy ones, the amount of spinning is small enough
 * not to be noticeable.
 * 
 * @author subes
 *
 */
@NotThreadSafe
public class BlockingMappedSynchronousReader extends AMappedSynchronousChannel
        implements ISynchronousReader<IByteBufferProvider> {
    private int lastTransaction;

    public BlockingMappedSynchronousReader(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        /*
         * use inital value instead of reading the value to not fall into a race condition when writer has already
         * written first message before the reader is initialized
         */
        lastTransaction = TRANSACTION_INITIAL_VALUE;
    }

    @Override
    public void close() throws IOException {
        if (mem != null) {
            setReadFinished(READFINISHED_CLOSED);
            super.close();
        }
    }

    @Override
    /**
     * This method is thread safe and does not require locking on the reader.
     */
    public boolean hasNext() throws IOException {
        if (mem == null) {
            return false;
        }
        final int curTransaction = getTransaction();
        if (curTransaction == TRANSACTION_CLOSED_VALUE) {
            throw FastEOFException.getInstance("Channel was closed by the other endpoint");
        }
        return curTransaction != lastTransaction && curTransaction != TRANSACTION_WRITING_VALUE
                && curTransaction != TRANSACTION_INITIAL_VALUE;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        lastTransaction = getTransaction();
        final int size = getSize();
        final IByteBuffer message = buffer.slice(MESSAGE_INDEX, size);
        return message;
    }

    @Override
    public void readFinished() {
        setReadFinished(READFINISHED_TRUE);
    }

}