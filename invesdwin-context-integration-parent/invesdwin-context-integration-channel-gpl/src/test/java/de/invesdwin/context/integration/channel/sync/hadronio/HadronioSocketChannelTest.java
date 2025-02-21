package de.invesdwin.context.integration.channel.sync.hadronio;

import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketChannelTest;
import de.invesdwin.context.integration.channel.sync.socket.tcp.SocketSynchronousChannel;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class HadronioSocketChannelTest extends SocketChannelTest {

    @Override
    protected HadronioSocketSynchronousChannel newSocketSynchronousChannel(final SocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize, final boolean lowLatency) {
        return new HadronioSocketSynchronousChannel(socketAddress, server, estimatedMaxMessageSize, lowLatency);
    }

    @Override
    protected ISynchronousReader<IByteBufferProvider> newSocketSynchronousReader(
            final SocketSynchronousChannel channel) {
        return new HadronioSocketSynchronousReader((HadronioSocketSynchronousChannel) channel);
    }

    @Override
    protected ISynchronousWriter<IByteBufferProvider> newSocketSynchronousWriter(
            final SocketSynchronousChannel channel) {
        return new HadronioSocketSynchronousWriter((HadronioSocketSynchronousChannel) channel);
    }

    @Override
    protected String newAddress() {
        return findLocalNetworkAddress();
    }

}
