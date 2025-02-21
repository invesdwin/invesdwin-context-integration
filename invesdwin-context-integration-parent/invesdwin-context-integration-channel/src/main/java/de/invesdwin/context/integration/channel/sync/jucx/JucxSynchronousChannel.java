package de.invesdwin.context.integration.channel.sync.jucx;

import java.io.Closeable;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.util.PrimitiveIterator;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.random.PseudoRandomGenerators;
import de.invesdwin.util.math.random.RandomAdapter;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ICloseableByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class JucxSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    public static final long TAG_MASK_ALL = 0xffffffffffffffffL;
    private static final long MIN_TAG = 0;
    private static final long MAX_TAG = 0x00ffffffffffffffL;

    private static final PrimitiveIterator.OfLong NEXT_TAG = new RandomAdapter(PseudoRandomGenerators.newPseudoRandom())
            .longs(MIN_TAG, MAX_TAG)
            .distinct()
            .iterator();

    protected final IJucxTransportType type;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected final UcpMemMapParams ucpMemMapParams;
    protected volatile boolean ucpEndpointOpening;
    protected final InetSocketAddress socketAddress;
    protected final boolean server;
    private final UcxSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();
    private final UcpRequestSpinWait requestSpinWait = new UcpRequestSpinWait(this);
    private long localTag;
    private long remoteTag;
    private long remoteAddress;
    private int remoteSocketSize;
    private final ErrorUcxCallback errorUcxCallback = new ErrorUcxCallback();

    public JucxSynchronousChannel(final IJucxTransportType type, final InetSocketAddress socketAddress,
            final boolean server, final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.ucpMemMapParams = newUcpMemMapParams();
        this.finalizer = new UcxSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    JucxSynchronousChannel(final JucxSynchronousChannelServer server, final UcpConnectionRequest connRequest)
            throws IOException {
        this.type = server.getType();
        this.socketAddress = server.socketAddress;
        this.server = false;
        this.estimatedMaxMessageSize = server.getEstimatedMaxMessageSize();
        this.socketSize = server.getSocketSize();
        this.ucpMemMapParams = newUcpMemMapParams();
        this.finalizer = new UcxSynchronousChannelFinalizer();
        finalizer.ucpContext = new UcpContext(newUcpContextParams());
        finalizer.closeables.push(finalizer.ucpContext);
        finalizer.ucpWorker = finalizer.ucpContext.newWorker(newUcpWorkerParams());
        finalizer.closeables.push(finalizer.ucpWorker);
        finalizer.ucpEndpoint = finalizer.ucpWorker
                .newEndpoint(newUcpEndpointParams().setConnectionRequest(connRequest));
        activeCount.incrementAndGet();
        finalizer.register(this);

        finalizer.ucpMemory = finalizer.ucpContext.memoryMap(getUcpMemMapParams());
        finalizer.closeables.push(finalizer.ucpMemory);
        try {
            establishConnection();
            finalizer.closeables.push(finalizer.ucpEndpoint);
        } catch (final IOException e) {
            close();
            throw e;
        } catch (final Throwable t) {
            close();
            throw Throwables.propagate(t);
        }
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + MESSAGE_INDEX;
    }

    protected UcpParams newUcpContextParams() {
        final UcpParams params = new UcpParams().requestWakeupFeature().setEstimatedNumEps(1).setMtWorkersShared(false);
        type.configureContextParams(params);
        return params;
    }

    protected UcpWorkerParams newUcpWorkerParams() {
        final UcpWorkerParams params = new UcpWorkerParams();
        type.configureWorkerParams(params);
        return params;
    }

    protected UcpMemMapParams newUcpMemMapParams() {
        final UcpMemMapParams params = new UcpMemMapParams().allocate().setLength(socketSize).nonBlocking();
        type.configureMemMapParams(params);
        return params;
    }

    protected UcpEndpointParams newUcpEndpointParams() {
        final UcpEndpointParams params = new UcpEndpointParams();
        params.setErrorHandler(errorUcxCallback::onError);
        type.configureEndpointParams(params);
        return params;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public IJucxTransportType getType() {
        return type;
    }

    public UcpMemMapParams getUcpMemMapParams() {
        return ucpMemMapParams;
    }

    public UcpContext getUcpContext() {
        return finalizer.ucpContext;
    }

    public UcpWorker getUcpWorker() {
        return finalizer.ucpWorker;
    }

    public UcpEndpoint getUcpEndpoint() {
        return finalizer.ucpEndpoint;
    }

    public UcpMemory getUcpMemory() {
        return finalizer.ucpMemory;
    }

    public long getRemoteAddress() {
        return remoteAddress;
    }

    public long getRemoteSocketSize() {
        return remoteSocketSize;
    }

    public UcpRemoteKey getUcpRemoteKey() {
        return finalizer.ucpRemoteKey;
    }

    public long getLocalTag() {
        return localTag;
    }

    public long getRemoteTag() {
        return remoteTag;
    }

    public ErrorUcxCallback getErrorUcxCallback() {
        return errorUcxCallback;
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

    public Stack<Closeable> getCloseables() {
        return finalizer.closeables;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitUcpEndpoint();
            return;
        }
        ucpEndpointOpening = true;
        final Duration connectTimeout = getConnectTimeout();
        final long startNanos = System.nanoTime();
        try {
            finalizer.ucpContext = new UcpContext(newUcpContextParams());
            finalizer.closeables.push(finalizer.ucpContext);
            finalizer.ucpWorker = finalizer.ucpContext.newWorker(newUcpWorkerParams());
            finalizer.closeables.push(finalizer.ucpWorker);
            finalizer.ucpMemory = finalizer.ucpContext.memoryMap(getUcpMemMapParams());
            finalizer.closeables.push(finalizer.ucpMemory);
            if (server) {
                final AtomicReference<UcpConnectionRequest> connRequest = new AtomicReference<>(null);
                finalizer.ucpListener = finalizer.ucpWorker.newListener(
                        new UcpListenerParams().setConnectionHandler(connRequest::set).setSockAddr(socketAddress));
                while (connRequest.get() == null) {
                    try {
                        finalizer.ucpWorker.progress();
                    } catch (final Exception e) {
                        throw new IOException(e);
                    }
                    if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                        try {
                            getMaxConnectRetryDelay().sleepRandom();
                        } catch (final InterruptedException e1) {
                            throw new IOException(e1);
                        }
                    }
                }
                //only allow one connection
                if (type.shouldCloseUcpListenerAfterAccept()) {
                    finalizer.ucpListener.close();
                    finalizer.ucpListener = null;
                }
                finalizer.ucpEndpoint = finalizer.ucpWorker
                        .newEndpoint(newUcpEndpointParams().setConnectionRequest(connRequest.get()));
                establishConnection();
                finalizer.closeables.push(finalizer.ucpEndpoint);
            } else {
                while (true) {
                    try {
                        finalizer.ucpEndpoint = finalizer.ucpWorker
                                .newEndpoint(newUcpEndpointParams().setSocketAddress(socketAddress));
                        establishConnection();
                        finalizer.closeables.push(finalizer.ucpEndpoint);
                        break;
                    } catch (final Throwable e) {
                        if (finalizer.ucpEndpoint != null) {
                            finalizer.ucpEndpoint.close();
                            finalizer.ucpEndpoint = null;
                        }
                        errorUcxCallback.reset();
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new IOException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
        } finally {
            ucpEndpointOpening = false;
        }
    }

    private void establishConnection() throws IOException {
        // Send worker and memory address and Rkey to receiver.
        final java.nio.ByteBuffer rkeyBuffer = finalizer.ucpMemory.getRemoteKeyBuffer();

        try (ICloseableByteBuffer expandableBuffer = ByteBuffers.DIRECT_EXPANDABLE_POOL.borrowObject()) {
            final int sendLength = Long.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + rkeyBuffer.capacity()
                    + Long.BYTES;
            expandableBuffer.ensureCapacity(sendLength);
            final java.nio.ByteBuffer buffer = expandableBuffer.nioByteBuffer();
            try {
                localTag = nextTag();
                buffer.putLong(localTag);
                final long localAddress = finalizer.ucpMemory.getAddress();
                buffer.putLong(localAddress);
                buffer.putInt(socketSize);
                buffer.putInt(rkeyBuffer.capacity());
                buffer.put(rkeyBuffer);
                final long localChecksum = checksum(buffer, 0, buffer.position());
                buffer.putLong(localChecksum);
                buffer.clear();

                //Exchanging tags and memory information to establish connection
                final UcpRequest sendRequest = type.establishConnectionSendNonBlocking(this,
                        expandableBuffer.addressOffset(), sendLength, errorUcxCallback.maybeThrowAndReset());
                requestSpinWait.waitForRequest(sendRequest, getConnectTimeout());
                final UcpRequest recvRequest = type.establishConnectionRecvNonBlocking(this,
                        expandableBuffer.addressOffset(), sendLength, errorUcxCallback.maybeThrowAndReset());
                requestSpinWait.waitForRequest(recvRequest, getConnectTimeout());

                Assertions.checkEquals(0, buffer.position());
                ByteBuffers.limit(buffer, Integers.checkedCast(recvRequest.getRecvSize()));

                remoteTag = buffer.getLong();
                remoteAddress = buffer.getLong();
                remoteSocketSize = buffer.getInt();
                if (remoteSocketSize != socketSize) {
                    throw new IllegalStateException(
                            "Remote socketSize mismatch: " + remoteSocketSize + " != " + socketSize);
                }
                final int remoteKeySize = buffer.getInt();
                final int checksumIndex = buffer.position() + remoteKeySize;
                final long remoteChecksum = buffer.getLong(checksumIndex);
                final long expectedChecksum = checksum(buffer, 0, checksumIndex);

                if (remoteChecksum != expectedChecksum) {
                    throw new IllegalStateException(
                            "Remote checksum mismatch: " + remoteChecksum + " != " + expectedChecksum);
                }

                ByteBuffers.limit(buffer, checksumIndex);
                finalizer.ucpRemoteKey = finalizer.ucpEndpoint.unpackRemoteKey(buffer);
                finalizer.closeables.push(finalizer.ucpRemoteKey);
            } finally {
                buffer.clear();
            }
        }
    }

    private void awaitUcpEndpoint() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.ucpEndpoint == null || ucpEndpointOpening) && activeCount.get() > 0) {
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
        remoteAddress = 0;
        remoteSocketSize = 0;
        remoteTag = 0;
        localTag = 0;
        errorUcxCallback.reset();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    public static synchronized long nextTag() {
        return NEXT_TAG.nextLong();
    }

    public static long checksum(final java.nio.ByteBuffer buffer, final int index, final int length) {
        final Checksum checksum = new CRC32();
        final int limit = index + length;
        for (int i = index; i < limit; i++) {
            checksum.update(buffer.get(i));
        }
        return checksum.getValue();
    }

    private static final class UcxSynchronousChannelFinalizer extends AWarningFinalizer {

        private UcpContext ucpContext;
        private UcpWorker ucpWorker;
        private UcpListener ucpListener;
        private UcpEndpoint ucpEndpoint;
        private UcpMemory ucpMemory;
        private UcpRemoteKey ucpRemoteKey;

        private final Stack<Closeable> closeables = new Stack<>();

        @Override
        protected void clean() {
            ucpRemoteKey = null;
            ucpMemory = null;
            ucpEndpoint = null;
            ucpListener = null;
            ucpWorker = null;
            ucpContext = null;
            while (!closeables.isEmpty()) {
                Closeables.closeQuietly(closeables.pop());
            }
        }

        @Override
        protected boolean isCleaned() {
            return ucpContext == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
