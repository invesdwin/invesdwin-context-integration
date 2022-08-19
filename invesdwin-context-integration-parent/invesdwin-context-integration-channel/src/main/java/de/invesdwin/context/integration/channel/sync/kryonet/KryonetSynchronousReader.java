package de.invesdwin.context.integration.channel.sync.kryonet;

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;

@NotThreadSafe
public class KryonetSynchronousReader extends AKryonetSynchronousChannel implements ISynchronousReader<IByteBuffer> {

    private volatile IByteBuffer polledValue;

    public KryonetSynchronousReader(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        super(address, tcpPort, udpPort, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
        connection.addListener(new Listener() {
            @Override
            public void received(final Connection connection, final Object object) {
                if (object instanceof IByteBuffer) {
                    polledValue = (IByteBuffer) object;
                }
            }
        });
    }

    @Override
    public boolean hasNext() throws IOException {
        return polledValue != null;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final IByteBuffer message = getPolledMessage();
        if (message != null && ClosedByteBuffer.isClosed(message)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message;
    }

    @Override
    public void readFinished() {
        //noop
    }

    private IByteBuffer getPolledMessage() {
        if (polledValue != null) {
            final IByteBuffer value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            if (polledValue != null) {
                final IByteBuffer value = polledValue;
                polledValue = null;
                return value;
            } else {
                return null;
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

}
