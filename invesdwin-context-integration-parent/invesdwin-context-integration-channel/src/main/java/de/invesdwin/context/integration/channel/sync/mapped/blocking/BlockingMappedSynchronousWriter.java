package de.invesdwin.context.integration.channel.sync.mapped.blocking;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeoutException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mapped.AMappedSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;
import de.invesdwin.util.time.duration.Duration;

/**
 * There should only be one writer per file, or else the threads might destroy each others data.
 *
 */
@NotThreadSafe
public class BlockingMappedSynchronousWriter extends AMappedSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    private final ASpinWait readFinishedWait = newSpinWait();
    private final Duration timeout;
    private IByteBuffer messageBuffer;

    public BlockingMappedSynchronousWriter(final File file, final int maxMessageSize, final Duration timeout) {
        super(file, maxMessageSize);
        this.timeout = timeout;
    }

    protected ASpinWait newSpinWait() {
        return new ASpinWait() {
            @Override
            public boolean isConditionFulfilled() throws Exception {
                return isReadFinished();
            }
        };
    }

    @Override
    public void open() throws IOException {
        super.open();
        //maybe remove closed flag that causes IOException on reader
        setTransaction(TRANSACTION_INITIAL_VALUE);
        setReadFinished(READFINISHED_TRUE);

        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        if (messageBuffer != null) {
            setTransaction(TRANSACTION_CLOSED_VALUE);
            setReadFinished(READFINISHED_CLOSED);
            super.close();
            messageBuffer = null;
        }
    }

    /**
     * Writes a message.
     *
     * @param message
     *            the message object to write
     * @throws EOFException
     *             in case the end of the file was reached
     */
    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        try {
            if (!readFinishedWait.awaitFulfill(System.nanoTime(), timeout)) {
                throw new TimeoutException("Write message timeout exceeded: " + timeout);
            }
        } catch (final IOException e) {
            throw e;
        } catch (final Exception e) {
            throw new IOException(e);
        }

        final byte nextTransaction = getNextTransaction();
        //open transaction
        setTransaction(TRANSACTION_WRITING_VALUE);
        setReadFinished(READFINISHED_FALSE);

        final int size = message.getBuffer(messageBuffer);
        setSize(size);

        //commit
        setTransaction(nextTransaction);
    }

}