package de.invesdwin.context.integration.channel.kryonet;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.message.EmptySynchronousMessage;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;

@NotThreadSafe
public class KryonetSynchronousReader extends AKryonetSynchronousChannel implements ISynchronousReader<byte[]> {

    private ImmutableSynchronousMessage<byte[]> polledValue;

    public KryonetSynchronousReader(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        super(address, tcpPort, udpPort, server);
    }

    @Override
    public void open() throws IOException {
        super.open();
        connection.addListener(new Listener() {
            @SuppressWarnings("unchecked")
            @Override
            public void received(final Connection connection, final Object object) {
                if (object instanceof ImmutableSynchronousMessage) {
                    polledValue = (ImmutableSynchronousMessage<byte[]>) object;
                }
            }
        });
    }

    @Override
    public boolean hasNext() throws IOException {
        return polledValue != null;
    }

    @Override
    public ISynchronousMessage<byte[]> readMessage() throws IOException {
        final ISynchronousMessage<byte[]> message = getPolledMessage();
        if (message.getType() == EmptySynchronousMessage.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousMessage<byte[]> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousMessage<byte[]> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            if (polledValue != null) {
                final ImmutableSynchronousMessage<byte[]> value = polledValue;
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
