package de.invesdwin.context.integration.channel.socket.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.context.integration.channel.message.ImmutableSynchronousMessage;

@NotThreadSafe
public class DatagramSocketSynchronousReader extends ADatagramSocketSynchronousChannel
        implements ISynchronousReader<byte[]> {

    public DatagramSocketSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public boolean hasNext() throws IOException {
        socket.receive(packet);
        return true;
    }

    @Override
    public ImmutableSynchronousMessage<byte[]> readMessage() throws IOException {
        final int type = getType();
        if (type == TYPE_CLOSED_VALUE) {
            throw new EOFException("Channel was closed by the other endpoint");
        }
        final int sequence = getSequence();
        final byte[] message = getMessage();
        return new ImmutableSynchronousMessage<byte[]>(type, sequence, message);
    }

}