package de.invesdwin.context.integration.channel.ipc;

import java.io.IOException;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousReaderSpinWait;
import de.invesdwin.context.integration.channel.sync.spinwait.SynchronousWriterSpinWait;

public interface ISynchronousChannelEndpoint<R, W> extends ISynchronousChannel {

    SynchronousReaderSpinWait<R> getReaderSpinWait();

    SynchronousWriterSpinWait<W> getWriterSpinWait();

    @Override
    default void open() throws IOException {
        getReaderSpinWait().getReader().open();
        getWriterSpinWait().getWriter().open();
    }

    @Override
    default void close() throws IOException {
        getWriterSpinWait().getWriter().close();
        getReaderSpinWait().getReader().close();
    }

}
