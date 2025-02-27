package de.invesdwin.context.integration.channel.stream.client.channel;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.stream.client.IStreamSynchronousEndpointClient;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.date.FDates;

@NotThreadSafe
public class StreamSynchronousEndpointClientWriter implements ISynchronousWriter<IByteBufferProvider> {

    public static final long CLOSED_INDEX = FDates.MAX_DATE.millisValue();

    private final StreamSynchronousEndpointClientChannel channel;
    private final boolean closeMessageEnabled;
    private IStreamSynchronousEndpointClient client;

    public StreamSynchronousEndpointClientWriter(final StreamSynchronousEndpointClientChannel channel) {
        this.channel = channel;
        this.closeMessageEnabled = channel.isCloseMessageEnabled();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        client = channel.getClient();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            if (closeMessageEnabled) {
                final ICloseableByteBuffer message;
                final Integer valueFixedLength = channel.getValueFixedLength();
                if (valueFixedLength == null) {
                    message = ClosedByteBuffer.INSTANCE;
                } else {
                    final ICloseableByteBuffer messageBuffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject();
                    messageBuffer.ensureCapacity(valueFixedLength);
                    final int closedBufferLength = ClosedByteBuffer.INSTANCE.capacity();
                    messageBuffer.clear(closedBufferLength, valueFixedLength - closedBufferLength);
                    messageBuffer.putBytes(0, ClosedByteBuffer.INSTANCE);
                    message = messageBuffer.sliceTo(valueFixedLength);
                }
                client.put(channel.getServiceId(), message);
            }
            client = null;
            channel.close();
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        //TODO: implement a variant that works without a copy
        final ICloseableByteBuffer messageCopy = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject();
        final int length = message.getBuffer(messageCopy);
        client.put(channel.getServiceId(), messageCopy.sliceTo(length));
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
