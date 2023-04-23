package de.invesdwin.context.integration.channel.ipc;

import java.io.IOException;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;

public interface ISynchronousChannelEndpoint<R, W> extends ISynchronousChannel {

    ISynchronousReader<R> getReader();

    ISynchronousWriter<W> getWriter();

    @Override
    default void open() throws IOException {
        getReader().open();
        getWriter().open();
    }

    @Override
    default void close() throws IOException {
        getWriter().close();
        getReader().close();
    }

}
