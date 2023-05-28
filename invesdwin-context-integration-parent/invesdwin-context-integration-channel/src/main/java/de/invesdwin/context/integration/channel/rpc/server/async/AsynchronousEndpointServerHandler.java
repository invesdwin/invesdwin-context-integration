package de.invesdwin.context.integration.channel.rpc.server.async;

import java.io.IOException;
import java.util.concurrent.Future;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.async.IAsynchronousHandler;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService;
import de.invesdwin.context.integration.channel.rpc.server.service.SynchronousEndpointService.ServerMethodInfo;
import de.invesdwin.context.integration.channel.rpc.server.service.command.IServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.ServiceSynchronousCommandSerde;
import de.invesdwin.context.integration.channel.rpc.server.service.command.deserializing.LazyDeserializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.server.service.command.serializing.LazySerializingServiceSynchronousCommand;
import de.invesdwin.util.concurrent.future.Futures;
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
    private final LazyDeserializingServiceSynchronousCommand<IByteBufferProvider> requestHolder = new LazyDeserializingServiceSynchronousCommand<>();
    private final LazySerializingServiceSynchronousCommand<Object> responseHolder = new LazySerializingServiceSynchronousCommand<Object>();
    private final ServiceSynchronousCommandSerde<IByteBufferProvider> outputSerde = new ServiceSynchronousCommandSerde<>(
            ByteBufferProviderSerde.GET, null);
    private LazySerializingServiceSynchronousCommand<Object> output;
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

    private LazySerializingServiceSynchronousCommand<Object> processResponse() {
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
        final int methodId = requestHolder.getMethod();
        final ServerMethodInfo methodInfo = service.getMethodInfo(methodId);
        if (methodInfo == null) {
            responseHolder.setService(serviceId);
            responseHolder.setMethod(IServiceSynchronousCommand.ERROR_METHOD_ID);
            responseHolder.setSequence(requestHolder.getSequence());
            responseHolder.setMessage(IServiceSynchronousCommand.ERROR_RESPONSE_SERDE_OBJ,
                    "method not found: " + methodId);
            return responseHolder;
        }

        //TODO: only execute in current thread when @Fast annotation is present, otherwise still use a different worker executor
        final Future<?> future = methodInfo.invoke(sessionId, requestHolder, responseHolder);
        if (future != null) {
            //TODO: don't block here, instead make accessible some form of async writer context
            Futures.waitNoInterrupt(future);
        }
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
