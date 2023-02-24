package de.invesdwin.context.integration.channel.sync.hadronio;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelector;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousReader;

@NotThreadSafe
public class HadronioSocketSynchronousReader extends SocketSynchronousReader {

    private SelectionKey selectionKey;

    public HadronioSocketSynchronousReader(final HadronioSocketSynchronousChannel channel) {
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
    protected boolean readFurther(final int targetPosition, final int readLength) throws IOException {
        selectionKey.selector().selectNow();
        return super.readFurther(targetPosition, readLength);
    }

}
