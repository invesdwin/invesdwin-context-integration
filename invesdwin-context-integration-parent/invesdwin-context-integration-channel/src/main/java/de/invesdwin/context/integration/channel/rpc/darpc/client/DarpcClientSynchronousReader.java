package de.invesdwin.context.integration.channel.rpc.darpc.client;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCFuture;
import com.ibm.darpc.DaRPCStream;

import de.invesdwin.context.integration.channel.rpc.darpc.RdmaRpcMessage;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DarpcClientSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private DarpcClientSynchronousChannel channel;
    private DaRPCStream<RdmaRpcMessage, RdmaRpcMessage> stream;
    private RdmaRpcMessage message;

    public DarpcClientSynchronousReader(final DarpcClientSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setReaderRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        stream = channel.getStream();
    }

    @Override
    public void close() {
        if (stream != null) {
            stream.clear();
            stream = null;
        }

        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean hasNext() throws IOException {
        if (stream == null) {
            throw FastEOFException.getInstance("socket closed");
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (message != null) {
            return true;
        }
        final DaRPCFuture<RdmaRpcMessage, RdmaRpcMessage> poll = stream.poll();
        if (poll != null) {
            if (!poll.isDone()) {
                throw new IllegalStateException("Not done");
            }
            //when there is no pending read, writes on the other side will never arrive
            message = poll.getReceiveMessage();
        }
        return message != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int size = message.getMessage().getInt(0);
        if (ClosedByteBuffer.isClosed(message.getMessage(), DarpcClientSynchronousChannel.MESSAGE_INDEX, size)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return message.getMessage().slice(DarpcClientSynchronousChannel.MESSAGE_INDEX, size);
    }

    @Override
    public void readFinished() {
        message = null;
    }

}
