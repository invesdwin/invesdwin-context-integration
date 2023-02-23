package de.invesdwin.context.integration.channel.sync.ucx.hadronio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelector;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousWriter;

@NotThreadSafe
public class HadronioSocketSynchronousWriter extends SocketSynchronousWriter {

    private SelectionKey selectionKey;

    public HadronioSocketSynchronousWriter(final HadronioSocketSynchronousChannel channel) {
        super(channel);
    }

    @Override
    public void open() throws IOException {
        super.open();
        final AbstractSelector selector = HadronioSocketSynchronousChannel.HADRONIO_PROVIDER.openSelector();
        selectionKey = socketChannel.register(selector, SelectionKey.OP_WRITE);
    }

    @Override
    public void close() throws IOException {
        super.close();
        if (selectionKey != null) {
            selectionKey.selector().close();
            selectionKey = null;
        }
    }

    @Override
    protected boolean writeFurther() throws IOException {
        final boolean writeFurther = super.writeFurther();
        selectionKey.selector().selectNow();
        return writeFurther;
    }

}
