package de.invesdwin.context.integration.channel.socket.udp;

import java.io.IOException;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.ISynchronousWriter;
import de.invesdwin.context.integration.channel.message.ISynchronousMessage;
import de.invesdwin.util.math.Bytes;

@NotThreadSafe
public class DatagramSocketSynchronousWriter extends ADatagramSocketSynchronousChannel
        implements ISynchronousWriter<byte[]> {

    public DatagramSocketSynchronousWriter(final SocketAddress socketAddress, final int estimatedMaxMessageSize) {
        super(socketAddress, false, estimatedMaxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (socket != null) {
            try {
                writeWithoutTypeCheck(TYPE_CLOSED_VALUE, SEQUENCE_CLOSED_VALUE, Bytes.EMPTY_ARRAY);
            } catch (final Throwable t) {
                //ignore
            }
        }
        super.close();
    }

    private void checkType(final int type) {
        if (type == TYPE_CLOSED_VALUE) {
            throw new IllegalArgumentException(
                    "type [" + type + "] is reserved for close notification, please use a different type number");
        }
    }

    @Override
    public void write(final int type, final int sequence, final byte[] message) throws IOException {
        checkType(type);
        writeWithoutTypeCheck(type, sequence, message);
    }

    @Override
    public void write(final ISynchronousMessage<byte[]> message) throws IOException {
        write(message.getType(), message.getSequence(), message.getMessage());
    }

    private void writeWithoutTypeCheck(final int type, final int sequence, final byte[] message) throws IOException {
        setType(type);
        setSequence(sequence);
        setMessage(message);
        socket.send(packet);
    }
}