package de.invesdwin.context.integration.channel.sync.mapped.blocking;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.mapped.AMappedSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.duration.Duration;

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
        implements ISynchronousReader<IByteBuffer> {
    private int lastTransaction;
    private final ASpinWait readFinishedWait = newSpinWait();
    private final Duration timeout;

    public BlockingMappedSynchronousReader(final File file, final int maxMessageSize, final Duration timeout) {
        super(file, maxMessageSize);
        this.timeout = timeout;
    }

    protected ASpinWait newSpinWait() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return hasNext();
            }
        };
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
        setReadFinished(READFINISHED_CLOSED);
        super.close();
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
            throw new EOFException("Channel was closed by the other endpoint");
        }
        return curTransaction != lastTransaction && curTransaction != TRANSACTION_WRITING_VALUE
                && curTransaction != TRANSACTION_INITIAL_VALUE;
    }

    @Override
    public IByteBuffer readMessage() {
        try {
            if (!readFinishedWait.awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Read message timeout exceeded: " + timeout);
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

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