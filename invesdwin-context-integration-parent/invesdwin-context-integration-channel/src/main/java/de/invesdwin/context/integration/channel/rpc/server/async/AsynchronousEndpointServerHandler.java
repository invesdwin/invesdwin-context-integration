package de.invesdwin.context.integration.channel.rpc.server.async;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.command.DeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.SerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.marshallers.serde.ByteBufferProviderSerde;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

@NotThreadSafe
public class AsynchronousEndpointServerHandler
        implements IAsynchronousHandler<IByteBufferProvider, IByteBufferProvider>, IByteBufferProvider {

    private final AsynchronousEndpointServerHandlerFactory parent;
    private final String sessionId;
    private final DeserializingServiceSynchronousCommand<IByteBufferProvider> requestHolder = new DeserializingServiceSynchronousCommand<>();
    private final SerializingServiceSynchronousCommand<Object> responseHolder = new SerializingServiceSynchronousCommand<Object>();
    private final ServiceSynchronousCommandSerde<IByteBufferProvider> outputSerde = new ServiceSynchronousCommandSerde<>(
            ByteBufferProviderSerde.GET, null);
    private SerializingServiceSynchronousCommand<Object> output;
    private IByteBuffer outputBuffer;
    private long lastHeartbeatNanos = System.nanoTime();

    public AsynchronousEndpointServerHandler(final AsynchronousEndpointServerHandlerFactory parent,
            final String sessionId) {
        this.parent = parent;
        this.sessionId = sessionId;
    }

    @Override
    public IByteBufferProvider open() throws IOException {
        //noop
        return null;
    }

    @Override
    public void close() throws IOException {
        requestHolder.close();
        responseHolder.close();
        output = null;
        outputBuffer = null;
    }

    @Override
    public IByteBufferProvider idle() throws IOException {
        if (isHeartbeatTimeout()) {
            throw FastEOFException.getInstance("heartbeat timeout [%s] exceeded", parent.getHeartbeatTimeout());
        }
        return null;
    }

    public boolean isHeartbeatTimeout() {
        return parent.getHeartbeatTimeout().isLessThanNanos(System.nanoTime() - lastHeartbeatNanos);
    }

    @Override
    public IByteBufferProvider handle(final IByteBufferProvider input) throws IOException {
        final IByteBuffer buffer = input.asBuffer();
        final int service = buffer.getInt(ServiceSynchronousCommandSerde.SERVICE_INDEX);
        final int method = buffer.getInt(ServiceSynchronousCommandSerde.METHOD_INDEX);
        final int sequence = buffer.getInt(ServiceSynchronousCommandSerde.SEQUENCE_INDEX);
        final int messageLength = buffer.capacity() - ServiceSynchronousCommandSerde.MESSAGE_INDEX;
        try {
            requestHolder.setService(service);
            requestHolder.setMethod(method);
            requestHolder.setSequence(sequence);
            requestHolder.setMessage(ByteBufferProviderSerde.GET,
                    buffer.slice(ServiceSynchronousCommandSerde.MESSAGE_INDEX, messageLength));
            //we expect async handlers to already be running inside of a worker pool, thus just execute in current thread
            output = processResponse();
            if (output != null) {
                return this;
            } else {
                return null;
            }
        } finally {
            requestHolder.close();
        }
    }

    private SerializingServiceSynchronousCommand<Object> processResponse() {
        lastHeartbeatNanos = System.nanoTime();
        final int serviceId = requestHolder.getService();
        if (serviceId == IServiceSynchronousCommand.HEARTBEAT_SERVICE_ID) {
            return null;
        }
        final SynchronousEndpointService service = parent.getService(serviceId);
        if (service == null) {
            responseHolder.setService(serviceId);
            responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            responseHolder.setSequence(requestHolder.getSequence());
            responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "service not found: " + serviceId);
            return responseHolder;
        }
        service.invoke(sessionId, requestHolder, responseHolder);
        return responseHolder;
    }

    @Override
    public void outputFinished() throws IOException {
        output = null;
        responseHolder.close();
    }

    @Override
    public int getBuffer(final IByteBuffer dst) {
        return outputSerde.toBuffer(dst, output);
    }

    @Override
    public IByteBuffer asBuffer() {
        if (outputBuffer == null) {
            //needs to be expandable so that FragmentSynchronousWriter can work properly
            outputBuffer = ByteBuffers.allocateExpandable();
        }
        final int length = getBuffer(outputBuffer);
        return outputBuffer.slice(0, length);
    }

}
