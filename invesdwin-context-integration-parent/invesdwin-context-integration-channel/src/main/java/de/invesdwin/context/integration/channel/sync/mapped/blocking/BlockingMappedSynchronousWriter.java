package de.invesdwin.context.integration.channel.sync.mapped.blocking;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.mapped.AMappedSynchronousChannel;
import de.invesdwin.util.concurrent.loop.ASpinWait;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.bytes.delegate.slice.SlicedFromDelegateByteBuffer;

/**
 * There should only be one writer per file, or else the threads might destroy each others data.
 *
 */
@NotThreadSafe
public class BlockingMappedSynchronousWriter extends AMappedSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private final ASpinWait readFinishedWait = newSpinWait();
    private IByteBuffer messageBuffer;

    public BlockingMappedSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
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
        setTransaction(TRANSACTION_CLOSED_VALUE);
        setReadFinished(READFINISHED_TRUE);
        super.close();
        messageBuffer = null;
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
    public void write(final IByteBufferWriter message) {
        try {
            readFinishedWait.awaitFulfill(System.nanoTime());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        final byte nextTransaction = getNextTransaction();
        //open transaction
        setTransaction(TRANSACTION_WRITING_VALUE);
        setReadFinished(READFINISHED_FALSE);

        final int size = message.writeBuffer(messageBuffer);
        setSize(size);

        //commit
        setTransaction(nextTransaction);
    }

}