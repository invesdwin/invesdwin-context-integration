package de.invesdwin.context.integration.channel.pipe.stream;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousChannel;

@NotThreadSafe
public abstract class AStreamPipeSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final File file;
    protected final int estimatedMaxMessageSize;
    protected final int fileSize;

    public AStreamPipeSynchronousChannel(final File file, final int estimatedMaxMessageSize) {
        this.file = file;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.fileSize = estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    protected EOFException newEofException(final IOException e) throws EOFException {
        final EOFException eof = new EOFException(e.getMessage());
        eof.initCause(e);
        return eof;
    }

}
