package de.invesdwin.context.integration.channel.sync.darpc.client;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCFuture;
import com.ibm.darpc.DaRPCStream;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.channel.sync.darpc.RdmaRpcMessage;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class DarpcClientSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private DarpcClientSynchronousChannel channel;
    private DaRPCStream<RdmaRpcMessage, RdmaRpcMessage> stream;
    private DaRPCFuture<RdmaRpcMessage, RdmaRpcMessage> future;
    private RdmaRpcMessage request;
    private IByteBuffer requestMessage;
    private RdmaRpcMessage response;

    public DarpcClientSynchronousWriter(final DarpcClientSynchronousChannel channel) {
        this.channel = channel;
        this.channel.setWriterRegistered();
    }

    @Override
    public void open() throws IOException {
        channel.open();
        stream = channel.getStream();
        request = channel.getRequest();
        response = channel.getResponse();
        requestMessage = request.getMessage().sliceFrom(DarpcClientSynchronousChannel.MESSAGE_INDEX);
    }

    @Override
    public void close() {
        if (stream != null) {
            try {
                writeAndFlushIfPossible(ClosedByteBuffer.INSTANCE);
            } catch (final Throwable t) {
                //ignore
            }
            stream.clear();
            stream = null;
            future = null;
            request = null;
            requestMessage = null;
            response = null;
        }

        if (channel != null) {
            channel.close();
            channel = null;
        }
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int size = message.getBuffer(requestMessage);
        request.getMessage().putInt(DarpcClientSynchronousChannel.SIZE_INDEX, size);
        future = stream.request(request, response, true);
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (future == null) {
            return true;
        } else if (future.isDone()) {
            future = null;
            return true;
        } else {
            return false;
        }
    }

}
