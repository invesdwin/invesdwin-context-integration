package de.invesdwin.context.integration.channel.sync.ucx.jucx;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import org.openucx.jucx.UcxException;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpConstants;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;
import org.openucx.jucx.ucs.UcsConstants;

import de.hhu.bsinfo.hadronio.util.TagUtil;
import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;
import de.invesdwin.util.error.Throwables;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AFinalizer;
import de.invesdwin.util.streams.buffer.bytes.ByteBufferAlignment;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class JucxSynchronousChannel implements ISynchronousChannel {

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    private static final int CONNECT_BUFFER_SIZE = Long.BYTES + Long.BYTES;
    private static final IObjectPool<IByteBuffer> CONNECT_BUFFER_POOL = new AgronaObjectPool<IByteBuffer>(
            () -> ByteBuffers.allocateDirectFixedAligned(CONNECT_BUFFER_SIZE, ByteBufferAlignment.PAGE));

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

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
    private final UcpRequestSpinWait requestSpinWait = new UcpRequestSpinWait();
    private long localTag;
    private long remoteTag;

    public JucxSynchronousChannel(final InetSocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.ucpMemMapParams = newUcpMemMapParams();
        this.finalizer = new UcxSynchronousChannelFinalizer(isPeerErrorHandlingMode());
        finalizer.register(this);
    }

    protected boolean isPeerErrorHandlingMode() {
        return true;
    }

    protected UcpParams newUcpContextParams() {
        return new UcpParams().requestWakeupFeature().requestRmaFeature().requestTagFeature();
    }

    protected UcpWorkerParams newUcpWorkerParams() {
        return new UcpWorkerParams();
    }

    protected UcpMemMapParams newUcpMemMapParams() {
        return new UcpMemMapParams().allocate().setLength(socketSize);
    }

    protected UcpEndpointParams newUcpEndpointParams() {
        final UcpEndpointParams ucpEndpointParams = new UcpEndpointParams();
        if (isPeerErrorHandlingMode()) {
            ucpEndpointParams.setPeerErrorHandlingMode();
        }
        return ucpEndpointParams;
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

    public long getLocalTag() {
        return localTag;
    }

    public long getRemoteTag() {
        return remoteTag;
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
            finalizer.ucpWorker = finalizer.ucpContext.newWorker(newUcpWorkerParams());

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
                finalizer.ucpListener.close();
                finalizer.ucpListener = null;
                finalizer.ucpEndpoint = finalizer.ucpWorker
                        .newEndpoint(newUcpEndpointParams().setConnectionRequest(connRequest.get()));
            } else {
                while (true) {
                    try {
                        finalizer.ucpEndpoint = finalizer.ucpWorker
                                .newEndpoint(newUcpEndpointParams().setErrorHandler((ep, status, errorMsg) -> {
                                    if (status == UcsConstants.STATUS.UCS_ERR_CONNECTION_RESET) {
                                        throw new ConnectException(errorMsg);
                                    } else {
                                        throw new UcxException(errorMsg);
                                    }
                                }).setSocketAddress(socketAddress));
                        break;
                    } catch (final Throwable e) {
                        if (finalizer.ucpEndpoint != null) {
                            finalizer.ucpEndpoint.close();
                            finalizer.ucpEndpoint = null;
                        }
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
            establishConnection();
        } finally {
            ucpEndpointOpening = false;
        }
    }

    void establishConnection() throws IOException {
        final IByteBuffer sendBuffer = CONNECT_BUFFER_POOL.borrowObject();
        final IByteBuffer receiveBuffer = CONNECT_BUFFER_POOL.borrowObject();
        try {
            localTag = TagUtil.generateId();
            final long localChecksum = TagUtil.calculateChecksum(localTag);
            sendBuffer.putLong(0, localTag);
            sendBuffer.putLong(Long.BYTES, localChecksum);

            //Exchanging tags to establish connection
            final UcpRequest sendRequest = finalizer.ucpEndpoint.sendStreamNonBlocking(sendBuffer.addressOffset(),
                    CONNECT_BUFFER_SIZE, null);
            final UcpRequest recvRequest = finalizer.ucpEndpoint.recvStreamNonBlocking(receiveBuffer.addressOffset(),
                    CONNECT_BUFFER_SIZE, UcpConstants.UCP_STREAM_RECV_FLAG_WAITALL, null);
            waitForRequest(sendRequest);
            waitForRequest(recvRequest);

            final long tag = recvRequest.getSenderTag();
            remoteTag = receiveBuffer.getLong(0);
            if (tag != remoteTag) {
                throw new IllegalStateException("Remote tag mismatch: " + remoteTag + " != " + tag);
            }
            final long remoteChecksum = receiveBuffer.getLong(Long.BYTES);
            final long expectedChecksum = TagUtil.calculateChecksum(remoteTag);
            if (remoteChecksum != expectedChecksum) {
                throw new IllegalStateException(
                        "Remote tag checksum mismatch: " + remoteChecksum + " != " + expectedChecksum);
            }
        } finally {
            CONNECT_BUFFER_POOL.returnObject(receiveBuffer);
            CONNECT_BUFFER_POOL.returnObject(sendBuffer);
        }
    }

    private void waitForRequest(final UcpRequest request) throws IOException {
        try {
            requestSpinWait.init(finalizer.ucpWorker, request);
            requestSpinWait.awaitFulfill(System.nanoTime(), getConnectTimeout());
        } catch (final Throwable t) {
            close();
            throw new IOException(t);
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
        synchronized (this) {
            if (activeCount.get() > 0) {
                activeCount.decrementAndGet();
            }
        }
        finalizer.close();
        localTag = 0;
        remoteTag = 0;
    }

    private static final class UcxSynchronousChannelFinalizer extends AFinalizer {

        private final boolean peerErrorHandlingMode;
        private final Exception initStackTrace;
        //TODO: should be a global per application
        private volatile UcpContext ucpContext;
        private volatile UcpWorker ucpWorker;
        private volatile UcpListener ucpListener;
        private volatile UcpEndpoint ucpEndpoint;

        protected UcxSynchronousChannelFinalizer(final boolean peerErrorHandlingMode) {
            this.peerErrorHandlingMode = peerErrorHandlingMode;
            if (Throwables.isDebugStackTraceEnabled()) {
                initStackTrace = new Exception();
                initStackTrace.fillInStackTrace();
            } else {
                initStackTrace = null;
            }
        }

        @Override
        protected void clean() {
            final UcpEndpoint ucpEndpointCopy = ucpEndpoint;
            if (ucpEndpointCopy != null) {
                ucpEndpoint = null;
                if (peerErrorHandlingMode) {
                    ucpEndpointCopy.closeNonBlockingForce();
                } else {
                    ucpEndpointCopy.closeNonBlockingFlush();
                }
                Closeables.closeQuietly(ucpEndpointCopy);
            }
            final UcpListener ucpListenerCopy = ucpListener;
            if (ucpListenerCopy != null) {
                ucpListener = null;
                Closeables.closeQuietly(ucpListenerCopy);
            }
            final UcpWorker workerCopy = ucpWorker;
            if (workerCopy != null) {
                ucpWorker = null;
                Closeables.closeQuietly(workerCopy);
            }
            final UcpContext contextCopy = ucpContext;
            if (contextCopy != null) {
                ucpContext = null;
                Closeables.closeQuietly(contextCopy);
            }
        }

        @Override
        protected void onRun() {
            String warning = "Finalizing unclosed " + JucxSynchronousChannel.class.getSimpleName();
            if (Throwables.isDebugStackTraceEnabled()) {
                final Exception stackTrace = initStackTrace;
                if (stackTrace != null) {
                    warning += " from stacktrace:\n" + Throwables.getFullStackTrace(stackTrace);
                }
            }
            new Log(this).warn(warning);
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
