package de.invesdwin.context.integration.channel.sync.kryonet;

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;

@NotThreadSafe
public class KryonetSynchronousWriter extends AKryonetSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

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
    public void write(final IByteBufferWriter message) throws IOException {
        connection.send(message);
    }

}
