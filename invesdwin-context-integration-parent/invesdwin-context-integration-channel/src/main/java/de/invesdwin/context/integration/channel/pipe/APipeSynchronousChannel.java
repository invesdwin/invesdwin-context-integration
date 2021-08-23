package de.invesdwin.context.integration.channel.pipe;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;
import de.invesdwin.util.marshallers.serde.ISerde;
import de.invesdwin.util.marshallers.serde.basic.IntegerSerde;

@NotThreadSafe
public abstract class APipeSynchronousChannel implements ISynchronousChannel {

    public static final int TYPE_POS = 0;
    public static final ISerde<Integer> TYPE_SERDE = IntegerSerde.GET;
    public static final int TYPE_OFFSET = TYPE_SERDE.toBytes(Integer.MAX_VALUE).length;
    public static final byte TYPE_CLOSED_VALUE = -1;

    public static final int SEQUENCE_POS = TYPE_POS + TYPE_OFFSET;
    public static final ISerde<Integer> SEQUENCE_SERDE = TYPE_SERDE;
    public static final int SEQUENCE_OFFSET = TYPE_OFFSET;
    public static final byte SEQUENCE_CLOSED_VALUE = -1;

    public static final int SIZE_POS = SEQUENCE_POS + SEQUENCE_OFFSET;
    public static final ISerde<Integer> SIZE_SERDE = SEQUENCE_SERDE;
    public static final int SIZE_OFFSET = SEQUENCE_OFFSET;

    public static final int MESSAGE_POS = SIZE_POS + SIZE_OFFSET;

    protected final File file;
    protected final int maxMessageSize;
    protected final int fileSize;

    public APipeSynchronousChannel(final File file, final int maxMessageSize) {
        this.file = file;
        this.maxMessageSize = maxMessageSize;
        this.fileSize = maxMessageSize + MESSAGE_POS;
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
