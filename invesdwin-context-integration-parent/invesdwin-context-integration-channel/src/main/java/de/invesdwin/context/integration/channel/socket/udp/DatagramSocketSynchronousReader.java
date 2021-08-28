package de.invesdwin.context.integration.channel.socket.udp;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousReader;
import de.invesdwin.util.lang.buffer.ClosedByteBuffer;
import de.invesdwin.util.lang.buffer.IByteBuffer;

@NotThreadSafe
public class DatagramSocketSynchronousReader extends ADatagramSocketSynchronousChannel
        implements ISynchronousReader<IByteBuffer> {

    public DatagramSocketSynchronousReader(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, true, estimatedMaxMessageSize);
    }

    @Override
    public boolean hasNext() throws IOException {
        socket.receive(packet);
        return true;
    }

    @Override
    public IByteBuffer readMessage() throws IOException {
        final int size = getSize();
        final IByteBuffer message = packetBuffer.slice(MESSAGE_INDEX, size);
        if (ClosedByteBuffer.isClosed(message)) {
            close();
            throw new EOFException("closed by other side");
        }
        return message;
    }

}
