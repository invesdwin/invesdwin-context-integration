package de.invesdwin.context.integration.channel.mapped;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SlicedFromDelegateByteBuffer;

/**
 * There should only be one writer per file, or else the threads might destroy each others data.
 *
 */
@NotThreadSafe
public class MappedSynchronousWriter extends AMappedSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    private IByteBuffer messageBuffer;

    public MappedSynchronousWriter(final File file, final int maxMessageSize) {
        super(file, maxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
        //maybe remove closed flag that causes IOException on reader
        setTransaction(TRANSACTION_INITIAL_VALUE);

        messageBuffer = new SlicedFromDelegateByteBuffer(buffer, MESSAGE_INDEX);
    }

    @Override
    public void close() throws IOException {
        setTransaction(TRANSACTION_CLOSED_VALUE);
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
        final byte nextTransaction = getNextTransaction();
        //open transaction
        setTransaction(TRANSACTION_WRITING_VALUE);

        final int size = message.write(messageBuffer);
        setSize(size);

        //commit
        setTransaction(nextTransaction);
    }

}