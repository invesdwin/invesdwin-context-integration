package de.invesdwin.context.integration.channel.rpc.server.async;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.server.async.handler.AsynchronousEndpointServerHandlerFactory;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceCommandSynchronousReader;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceCommandSynchronousWriter;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@ThreadSafe
public class AsynchronousEndpointServer implements ISynchronousChannel {

    private final AsynchronousEndpointServerHandlerFactory handlerFactory;
    private final IAsynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, Object> serverSessionFactory;
    private IAsynchronousEndpoint<IByteBufferProvider, IByteBufferProvider, Object> serverEndpoint;
    private ISynchronousReader<IServiceSynchronousCommand<IByteBufferProvider>> reader;
    private ISynchronousWriter<IServiceSynchronousCommand<IByteBufferProvider>> writer;
    private IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider> handler;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public AsynchronousEndpointServer(
            final IAsynchronousEndpointFactory<IByteBufferProvider, IByteBufferProvider, ?> serverSessionFactory,
            final AsynchronousEndpointServerHandlerFactory handlerFactory) {
        this.serverSessionFactory = (IAsynchronousEndpointFactory) serverSessionFactory;
        this.handlerFactory = handlerFactory;
    }

    @Override
    public void open() throws IOException {
        this.serverEndpoint = serverSessionFactory.newEndpoint();
        this.reader = new ServiceCommandSynchronousReader<IByteBufferProvider>(serverEndpoint.getReader(),
                ByteBufferProviderSerde.GET);
        this.writer = new ServiceCommandSynchronousWriter<IByteBufferProvider>(serverEndpoint.getWriter(),
                ByteBufferProviderSerde.GET, ByteBuffers.EXPANDABLE_LENGTH);
        this.reader.open();
        this.writer.open();
        this.handler = handlerFactory.newHandler();
    }

    @Override
    public void close() throws IOException {
        if (handler != null) {
            handler.close();
            handler = null;
        }
        if (reader != null) {
            reader.close();
            reader = null;
        }
        if (writer != null) {
            writer.close();
            writer = null;
        }
        if (serverEndpoint != null) {
            serverEndpoint.close();
            serverEndpoint = null;
        }
    }

}
