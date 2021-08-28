package de.invesdwin.context.integration.channel.kryonet;

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;

@NotThreadSafe
public class KryonetSynchronousWriter extends AKryonetSynchronousChannel implements ISynchronousWriter<IByteBuffer> {

    public KryonetSynchronousWriter(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        super(address, tcpPort, udpPort, server);
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                write(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final IByteBuffer message) throws IOException {
        connection.send(message);
    }

}
