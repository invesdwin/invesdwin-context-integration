package de.invesdwin.context.integration.channel.socket.udp;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBuffer;
import de.invesdwin.util.streams.buffer.IByteBufferWriter;
import de.invesdwin.util.streams.buffer.delegate.slice.SliceFromDelegateByteBuffer;
import de.invesdwin.util.streams.buffer.extend.ExpandableArrayByteBuffer;

@NotThreadSafe
public class DatagramSocketSynchronousWriter extends ADatagramSocketSynchronousChannel
        implements ISynchronousWriter<IByteBufferWriter> {

    protected ExpandableArrayByteBuffer packetBuffer = new ExpandableArrayByteBuffer();
    protected IByteBuffer messageBuffer = new SliceFromDelegateByteBuffer(packetBuffer, MESSAGE_INDEX);
    protected DatagramPacket packet;

    public DatagramSocketSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, false, estimatedMaxMessageSize);
    }

    @Override
    public void open() throws IOException {
        super.open();
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
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
        final int size = message.write(messageBuffer);
        packetBuffer.putInt(SIZE_INDEX, size);
        packet.setData(packetBuffer.byteArray(), 0, MESSAGE_INDEX + size);
        socket.send(packet);
    }

}
