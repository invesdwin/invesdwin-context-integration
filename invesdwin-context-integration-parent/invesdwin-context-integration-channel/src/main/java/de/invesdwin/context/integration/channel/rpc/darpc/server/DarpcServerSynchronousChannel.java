package de.invesdwin.context.integration.channel.rpc.darpc.server;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.darpc.DaRPCServerEndpoint;
import com.ibm.darpc.DaRPCServerGroup;
import com.ibm.disni.RdmaEndpoint;
import com.ibm.disni.RdmaEndpointGroup;
import com.ibm.disni.RdmaServerEndpoint;
import com.ibm.disni.verbs.IbvMr;

import de.invesdwin.context.integration.channel.rpc.darpc.RdmaRpcMessage;
import de.invesdwin.context.integration.channel.rpc.darpc.RdmaRpcService;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class DarpcServerSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private final DisniSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public DarpcServerSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.finalizer = new DisniSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public DaRPCServerGroup<RdmaRpcMessage, RdmaRpcMessage> getEndpointGroup() {
        return finalizer.endpointGroup;
    }

    public DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> getEndpoint() {
        return finalizer.endpoint;
    }

    public RdmaServerEndpoint<DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage>> getServerEndpoint() {
        return finalizer.serverEndpoint;
    }

    public boolean isReaderRegistered() {
        return readerRegistered;
    }

    public void setReaderRegistered() {
        if (readerRegistered) {
            throw new IllegalStateException("reader already registered");
        }
        this.readerRegistered = true;
    }

    public boolean isWriterRegistered() {
        return writerRegistered;
    }

    public void setWriterRegistered() {
        if (writerRegistered) {
            throw new IllegalStateException("writer already registered");
        }
        this.writerRegistered = true;
    }

    public Stack<IbvMr> getMemoryRegions() {
        return finalizer.memoryRegions;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitSocketChannel();
            return;
        }
        socketChannelOpening = true;
        final boolean blocking = true;
        final RdmaRpcService rpcService = new RdmaRpcService(socketSize);
        try {
            finalizer.endpointGroup = DaRPCServerGroup.createServerGroup(rpcService, new long[] { 0L }, -1, 0,
                    !blocking, 16, 16, 16, 32);
        } catch (final Throwable t) {
            throw new IOException(t);
        }
        try {
            if (server) {
                finalizer.serverEndpoint = finalizer.endpointGroup.createServerEndpoint();
                try {
                    finalizer.serverEndpoint.bind(socketAddress, 1);
                    finalizer.endpoint = finalizer.serverEndpoint.accept();
                } catch (final Throwable t) {
                    throw new IOException(t);
                }
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    finalizer.endpoint = finalizer.endpointGroup.createEndpoint();
                    try {
                        finalizer.endpoint.connect(socketAddress, getConnectTimeout().intValue(FTimeUnit.MILLISECONDS));
                        break;
                    } catch (final Throwable e) {
                        Closeables.closeQuietly(finalizer.endpoint);
                        finalizer.endpoint = null;
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new IOException(e1);
                            }
                        } else {
                            throw new IOException(e);
                        }
                    }
                }
            }
        } finally {
            socketChannelOpening = false;
        }
    }

    private void awaitSocketChannel() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.endpoint == null || socketChannelOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new IOException(t);
        }
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    protected SocketChannel newSocketChannel() throws IOException {
        return SocketChannel.open();
    }

    protected ServerSocketChannel newServerSocketChannel() throws IOException {
        return ServerSocketChannel.open();
    }

    protected Duration getMaxConnectRetryDelay() {
        return SynchronousChannels.DEFAULT_MAX_RECONNECT_DELAY;
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected Duration getWaitInterval() {
        return SynchronousChannels.DEFAULT_WAIT_INTERVAL;
    }

    @Override
    public void close() {
        if (!shouldClose()) {
            return;
        }
        finalizer.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private static final class DisniSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile DaRPCServerGroup<RdmaRpcMessage, RdmaRpcMessage> endpointGroup;
        private volatile DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage> endpoint;
        private volatile RdmaServerEndpoint<DaRPCServerEndpoint<RdmaRpcMessage, RdmaRpcMessage>> serverEndpoint;
        private final Stack<IbvMr> memoryRegions = new Stack<>();

        @Override
        protected void clean() {
            while (!memoryRegions.isEmpty()) {
                try {
                    endpoint.deregisterMemory(memoryRegions.pop());
                } catch (final IOException e) {
                    //ignore
                }
            }
            final RdmaEndpoint endpointCopy = endpoint;
            if (endpointCopy != null) {
                endpoint = null;
                Closeables.closeQuietly(endpointCopy);
            }
            final RdmaServerEndpoint<? extends RdmaEndpoint> serverEndpointCopy = serverEndpoint;
            if (serverEndpointCopy != null) {
                serverEndpoint = null;
                Closeables.closeQuietly(serverEndpointCopy);
            }
            final RdmaEndpointGroup<? extends RdmaEndpoint> endpointGroupCopy = endpointGroup;
            if (endpointGroupCopy != null) {
                endpointGroup = null;
                Closeables.closeQuietly(endpointGroupCopy);
            }
        }

        @Override
        protected boolean isCleaned() {
            return endpoint == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
