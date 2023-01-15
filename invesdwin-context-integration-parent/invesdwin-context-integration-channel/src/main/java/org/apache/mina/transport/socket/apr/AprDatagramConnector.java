package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.polling.AbstractPollingIoConnector;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.service.SimpleIoProcessorPool;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.transport.socket.DefaultSocketSessionConfig;
import org.apache.mina.transport.socket.SocketConnector;
import org.apache.mina.transport.socket.SocketSessionConfig;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * {@link IoConnector} for APR based socket transport (TCP/IP).
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@ThreadSafe
public final class AprDatagramConnector extends AbstractPollingIoConnector<AprSession, Long>
        implements SocketConnector {

    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;

    private static final int POLLSET_SIZE = 1024;

    private final Map<Long, ConnectionRequest> requests = new HashMap<Long, ConnectionRequest>(POLLSET_SIZE);

    private final Object wakeupLock = new Object();

    private volatile long wakeupSocket;

    private volatile boolean toBeWakenUp;

    private volatile long pool;

    private volatile long pollset; // socket poller

    private final long[] polledSockets = new long[POLLSET_SIZE << 1];

    private final Queue<Long> polledHandles = new ConcurrentLinkedQueue<Long>();

    private final Set<Long> failedHandles = new HashSet<Long>(POLLSET_SIZE);

    private volatile java.nio.ByteBuffer dummyBuffer;

    /**
     * Create an {@link AprDatagramConnector} with default configuration (multiple thread model).
     */
    public AprDatagramConnector() {
        super(new DefaultSocketSessionConfig(), AprDatagramIoProcessor.class);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprDatagramConnector} with default configuration, and given number of
     * {@link AprDatagramIoProcessor} for multithreading I/O operations
     *
     * @param processorCount
     *            the number of processor to create and place in a {@link SimpleIoProcessorPool}
     */
    public AprDatagramConnector(final int processorCount) {
        super(new DefaultSocketSessionConfig(), AprDatagramIoProcessor.class, processorCount);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprDatagramConnector} with default configuration but a specific {@link IoProcessor},
     * useful for sharing the same processor over multiple {@link IoService} of the same type.
     *
     * @param processor
     *            the processor to use for managing I/O events
     */
    public AprDatagramConnector(final IoProcessor<AprSession> processor) {
        super(new DefaultSocketSessionConfig(), processor);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprDatagramConnector} with a given {@link Executor} for handling connection events and a
     * given {@link IoProcessor} for handling I/O events, useful for sharing the same processor and executor over
     * multiple {@link IoService} of the same type.
     *
     * @param executor
     *            the executor for connection
     * @param processor
     *            the processor for I/O operations
     */
    public AprDatagramConnector(final Executor executor, final IoProcessor<AprSession> processor) {
        super(new DefaultSocketSessionConfig(), executor, processor);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init() throws Exception {
        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibrary.getInstance().getRootPool());

        wakeupSocket = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP, pool);

        dummyBuffer = Pool.alloc(pool, 1);

        pollset = Poll.create(POLLSET_SIZE, pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);

        if (pollset <= 0) {
            pollset = Poll.create(62, pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);
        }

        if (pollset <= 0) {
            if (Status.APR_STATUS_IS_ENOTIMPL(-(int) pollset)) {
                throw new RuntimeIoException("Thread-safe pollset is not supported in this platform.");
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws Exception {
        if (wakeupSocket > 0) {
            Socket.close(wakeupSocket);
        }
        if (pollset > 0) {
            Poll.destroy(pollset);
        }
        if (pool > 0) {
            Pool.destroy(pool);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<Long> allHandles() {
        return polledHandles.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean connect(final Long handle, final SocketAddress remoteAddress) throws Exception {
        final InetSocketAddress ra = (InetSocketAddress) remoteAddress;
        final long sa;
        if (ra != null) {
            if (ra.getAddress() == null) {
                sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, ra.getPort(), 0, pool);
            } else {
                sa = Address.info(ra.getAddress().getHostAddress(), Socket.APR_INET, ra.getPort(), 0, pool);
            }
        } else {
            sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, 0, 0, pool);
        }

        final int rv = Socket.connect(handle, sa);
        if (rv == Status.APR_SUCCESS) {
            return true;
        }

        if (Status.APR_STATUS_IS_EINPROGRESS(rv)) {
            return false;
        }

        throwException(rv);
        throw new InternalError(); // This sentence will never be executed.
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ConnectionRequest getConnectionRequest(final Long handle) {
        return requests.get(handle);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void close(final Long handle) throws Exception {
        finishConnect(handle);
        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean finishConnect(final Long handle) throws Exception {
        Poll.remove(pollset, handle);
        requests.remove(handle);
        if (failedHandles.remove(handle)) {
            final int rv = Socket.recvb(handle, dummyBuffer, 0, 1);
            throwException(rv);
            throw new InternalError("Shouldn't reach here.");
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Long newHandle(final SocketAddress localAddress) throws Exception {
        final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP, pool);
        boolean success = false;
        try {
            int result = Socket.optSet(handle, Socket.APR_SO_NONBLOCK, 1);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }
            result = Socket.timeoutSet(handle, 0);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }

            if (localAddress != null) {
                final InetSocketAddress la = (InetSocketAddress) localAddress;
                final long sa;

                if (la.getAddress() == null) {
                    sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, la.getPort(), 0, pool);
                } else {
                    sa = Address.info(la.getAddress().getHostAddress(), Socket.APR_INET, la.getPort(), 0, pool);
                }

                result = Socket.bind(handle, sa);
                if (result != Status.APR_SUCCESS) {
                    throwException(result);
                }
            }

            success = true;
            return handle;
        } finally {
            if (!success) {
                final int rv = Socket.close(handle);
                if (rv != Status.APR_SUCCESS) {
                    throwException(rv);
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AprSession newSession(final IoProcessor<AprSession> processor, final Long handle) throws Exception {
        return new AprSocketSession(this, processor, handle);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void register(final Long handle, final ConnectionRequest request) throws Exception {
        final int rv = Poll.add(pollset, handle, Poll.APR_POLLOUT);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }

        requests.put(handle, request);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int select(final int timeout) throws Exception {
        int rv = Poll.poll(pollset, timeout * 1000, polledSockets, false);
        if (rv <= 0) {
            if (rv != APR_TIMEUP_ERROR) {
                throwException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    Poll.add(pollset, polledSockets[i], Poll.APR_POLLOUT);
                }
            } else if (rv < 0) {
                throwException(rv);
            }

            return 0;
        } else {
            rv <<= 1;
            if (!polledHandles.isEmpty()) {
                polledHandles.clear();
            }

            for (int i = 0; i < rv; i++) {
                final long flag = polledSockets[i];
                final long socket = polledSockets[++i];
                if (socket == wakeupSocket) {
                    synchronized (wakeupLock) {
                        Poll.remove(pollset, wakeupSocket);
                        toBeWakenUp = false;
                    }
                    continue;
                }
                polledHandles.add(socket);
                if ((flag & Poll.APR_POLLOUT) == 0) {
                    failedHandles.add(socket);
                }
            }
            return polledHandles.size();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<Long> selectedHandles() {
        return polledHandles.iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void wakeup() {
        if (toBeWakenUp) {
            return;
        }

        // Add a dummy socket to the pollset.
        synchronized (wakeupLock) {
            toBeWakenUp = true;
            Poll.add(pollset, wakeupSocket, Poll.APR_POLLOUT);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TransportMetadata getTransportMetadata() {
        return AprSocketSession.METADATA;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public SocketSessionConfig getSessionConfig() {
        return (SocketSessionConfig) sessionConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getDefaultRemoteAddress() {
        return (InetSocketAddress) super.getDefaultRemoteAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setDefaultRemoteAddress(final InetSocketAddress defaultRemoteAddress) {
        super.setDefaultRemoteAddress(defaultRemoteAddress);
    }

    /**
     * transform an APR error number in a more fancy exception
     *
     * @param code
     *            APR error code
     * @throws IOException
     *             the produced exception for the given APR error number
     */
    private void throwException(final int code) throws IOException {
        throw new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }
}
