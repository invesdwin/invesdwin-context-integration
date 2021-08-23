package de.invesdwin.context.integration.channel.kryonet;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;

@NotThreadSafe
public class KryonetSynchronousReader extends AKryonetSynchronousChannel implements ISynchronousReader<byte[]> {

    private ImmutableSynchronousCommand<byte[]> polledValue;

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
                if (object instanceof ImmutableSynchronousCommand) {
                    polledValue = (ImmutableSynchronousCommand<byte[]>) object;
                }
            }
        });
    }

    @Override
    public boolean hasNext() throws IOException {
        return polledValue != null;
    }

    @Override
    public ISynchronousCommand<byte[]> readMessage() throws IOException {
        final ISynchronousCommand<byte[]> message = getPolledMessage();
        if (message.getType() == EmptySynchronousCommand.TYPE) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

    private ISynchronousCommand<byte[]> getPolledMessage() {
        if (polledValue != null) {
            final ImmutableSynchronousCommand<byte[]> value = polledValue;
            polledValue = null;
            return value;
        }
        try {
            if (polledValue != null) {
                final ImmutableSynchronousCommand<byte[]> value = polledValue;
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
