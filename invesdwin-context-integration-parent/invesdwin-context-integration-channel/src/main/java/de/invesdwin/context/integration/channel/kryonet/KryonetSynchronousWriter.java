package de.invesdwin.context.integration.channel.kryonet;

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.command.EmptySynchronousCommand;
import de.invesdwin.context.integration.channel.command.ISynchronousCommand;
import de.invesdwin.context.integration.channel.command.ImmutableSynchronousCommand;

@NotThreadSafe
public class KryonetSynchronousWriter extends AKryonetSynchronousChannel implements ISynchronousWriter<byte[]> {

    public KryonetSynchronousWriter(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        super(address, tcpPort, udpPort, server);
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                write(EmptySynchronousCommand.getInstance());
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        write(new ImmutableSynchronousCommand<byte[]>(type, sequence, message));
    }

    @Override
    public void write(final ISynchronousCommand<byte[]> message) throws IOException {
        connection.send(message);
    }

}
