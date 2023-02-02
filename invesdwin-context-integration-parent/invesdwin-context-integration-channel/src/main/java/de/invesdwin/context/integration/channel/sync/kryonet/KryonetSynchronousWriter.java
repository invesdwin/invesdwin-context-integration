package de.invesdwin.context.integration.channel.sync.kryonet;

import java.io.IOException;
import java.net.InetAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class KryonetSynchronousWriter extends AKryonetSynchronousChannel
        implements ISynchronousWriter<IByteBufferProvider> {

    public KryonetSynchronousWriter(final InetAddress address, final int tcpPort, final int udpPort,
            final boolean server) {
        super(address, tcpPort, udpPort, server);
    }

    @Override
    public void close() throws IOException {
        if (connection != null) {
            try {
                writeAndFinishIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        connection.send(message);
    }

    @Override
    public boolean writeFinished() throws IOException {
        return true;
    }

}
