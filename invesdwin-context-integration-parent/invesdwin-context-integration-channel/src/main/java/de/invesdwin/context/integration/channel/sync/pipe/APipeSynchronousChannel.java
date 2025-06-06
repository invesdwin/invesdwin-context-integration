package de.invesdwin.context.integration.channel.sync.pipe;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;

@NotThreadSafe
public abstract class APipeSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final File file;
    protected final int estimatedMaxMessageSize;
    protected final int fileSize;
    protected final boolean closeMessageEnabled;

    public APipeSynchronousChannel(final File file, final int estimatedMaxMessageSize) {
        this.file = file;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.fileSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.closeMessageEnabled = newCloseMessageEnabled();
    }

    protected boolean newCloseMessageEnabled() {
        return true;
    }

}
