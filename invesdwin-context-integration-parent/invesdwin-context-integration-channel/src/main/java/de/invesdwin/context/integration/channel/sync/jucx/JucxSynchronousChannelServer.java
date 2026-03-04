package de.invesdwin.context.integration.channel.sync.jucx;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.util.Stack;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;
import org.openucx.jucx.ucp.UcpConnectionRequest;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.jucx.type.IJucxTransportType;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.streams.closeable.Closeables;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class JucxSynchronousChannelServer implements ISynchronousReader<JucxSynchronousChannel> {

    protected IJucxTransportType type;
    protected final InetSocketAddress socketAddress;
    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    private final UcpMemMapParams ucpMemMapParams;
    private final JucxSynchronousChannelFinalizer finalizer;

    public JucxSynchronousChannelServer(final IJucxTransportType type, final InetSocketAddress socketAddress,
            final int estimatedMaxMessageSize) {
        this.type = type;
        this.socketAddress = socketAddress;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = newSocketSize(estimatedMaxMessageSize);
        this.ucpMemMapParams = newUcpMemMapParams();
        this.finalizer = new JucxSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    protected int newSocketSize(final int estimatedMaxMessageSize) {
        return estimatedMaxMessageSize + JucxSynchronousChannel.MESSAGE_INDEX;
    }

    public IJucxTransportType getType() {
        return type;
    }

    public InetSocketAddress getSocketAddress() {
        return socketAddress;
    }

    public int getEstimatedMaxMessageSize() {
        return estimatedMaxMessageSize;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public UcpListener getUcpListener() {
        return finalizer.ucpListener;
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

    @Override
    public void open() throws IOException {
        if (finalizer.ucpListener != null) {
            throw new IllegalStateException("Already opened");
        }

        finalizer.ucpContext = new UcpContext(newUcpContextParams());
        finalizer.closeables.push(finalizer.ucpContext);
        finalizer.ucpWorker = finalizer.ucpContext.newWorker(newUcpWorkerParams());
        finalizer.closeables.push(finalizer.ucpWorker);
        finalizer.ucpListener = finalizer.ucpWorker
                .newListener(new UcpListenerParams().setConnectionHandler((connRequest) -> {
                    finalizer.pendingConnRequests.add(connRequest);
                }).setSockAddr(socketAddress));
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected ServerSocketChannel newServerEndpoint() throws IOException {
        return ServerSocketChannel.open();
    }

    @Override
    public void close() {
        finalizer.close();
    }

    private static final class JucxSynchronousChannelFinalizer extends AWarningFinalizer {

        private UcpContext ucpContext;
        private UcpWorker ucpWorker;
        private volatile UcpListener ucpListener;
        private ManyToOneConcurrentLinkedQueue<UcpConnectionRequest> pendingConnRequests = new ManyToOneConcurrentLinkedQueue<>();

        private final Stack<Closeable> closeables = new Stack<>();

        @Override
        protected void clean() {
            if (pendingConnRequests != null) {
                while (!pendingConnRequests.isEmpty()) {
                    final UcpConnectionRequest pendingConnRequestCopy = pendingConnRequests.remove();
                    if (pendingConnRequestCopy != null) {
                        pendingConnRequestCopy.reject();
                    }
                }
                pendingConnRequests = null;
            }
            ucpListener = null;
            ucpWorker = null;
            ucpContext = null;
            while (!closeables.isEmpty()) {
                Closeables.closeQuietly(closeables.pop());
            }
        }

        @Override
        protected boolean isCleaned() {
            return ucpListener == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

    @Override
    public boolean hasNext() throws IOException {
        try {
            finalizer.ucpWorker.progress();
        } catch (final Exception e) {
            throw new IOException(e);
        }
        if (!finalizer.pendingConnRequests.isEmpty()) {
            return true;
        }
        final UcpListener ucpListenerCopy = finalizer.ucpListener;
        if (ucpListenerCopy == null) {
            throw FastEOFException.getInstance("closed");
        }
        try {
            finalizer.ucpWorker.progress();
        } catch (final Exception e) {
            throw new IOException(e);
        }
        return !finalizer.pendingConnRequests.isEmpty();
    }

    @Override
    public JucxSynchronousChannel readMessage() throws IOException {
        final UcpConnectionRequest connRequest = finalizer.pendingConnRequests.remove();
        final JucxSynchronousChannel channel = newJucxSynchronousChannel(connRequest);
        return channel;
    }

    protected JucxSynchronousChannel newJucxSynchronousChannel(final UcpConnectionRequest connRequest)
            throws IOException {
        return new JucxSynchronousChannel(this, connRequest);
    }

    @Override
    public void readFinished() throws IOException {
        //noop
    }

}
