package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.polling.AbstractPollingIoAcceptor;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.IoService;
import org.apache.mina.core.service.SimpleIoProcessorPool;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.transport.socket.DefaultSocketSessionConfig;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * {@link IoAcceptor} for APR based socket transport (TCP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
public final class AprSctpAcceptor extends AbstractPollingIoAcceptor<AprSession, Long> {
    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;

    private static final int POLLSET_SIZE = 1024;

    private final Object wakeupLock = new Object();

    private volatile long wakeupSocket;

    private volatile boolean toBeWakenUp;

    private volatile long pool;

    private volatile long pollset; // socket poller

    private final long[] polledSockets = new long[POLLSET_SIZE << 1];

    private final Queue<Long> polledHandles = new ConcurrentLinkedQueue<Long>();

    /**
     * Constructor for {@link AprSctpAcceptor} using default parameters (multiple thread model).
     */
    public AprSctpAcceptor() {
        super(new DefaultSocketSessionConfig(), AprIoProcessor.class);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprSctpAcceptor} using default parameters, and given number of {@link AprIoProcessor} for
     * multithreading I/O operations.
     * 
     * @param processorCount
     *            the number of processor to create and place in a {@link SimpleIoProcessorPool}
     */
    public AprSctpAcceptor(final int processorCount) {
        super(new DefaultSocketSessionConfig(), AprIoProcessor.class, processorCount);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprSctpAcceptor} with default configuration but a specific {@link AprIoProcessor}, useful
     * for sharing the same processor over multiple {@link IoService} of the same type.
     *
     * @param processor
     *            the processor to use for managing I/O events
     */
    public AprSctpAcceptor(final IoProcessor<AprSession> processor) {
        super(new DefaultSocketSessionConfig(), processor);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * Constructor for {@link AprSctpAcceptor} with a given {@link Executor} for handling connection events and a given
     * {@link AprIoProcessor} for handling I/O events, useful for sharing the same processor and executor over multiple
     * {@link IoService} of the same type.
     *
     * @param executor
     *            the executor for connection
     * @param processor
     *            the processor for I/O operations
     */
    public AprSctpAcceptor(final Executor executor, final IoProcessor<AprSession> processor) {
        super(new DefaultSocketSessionConfig(), executor, processor);
        ((DefaultSocketSessionConfig) getSessionConfig()).init(this);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AprSession accept(final IoProcessor<AprSession> processor, final Long handle) throws Exception {
        final long s = Socket.accept(handle);
        boolean success = false;
        try {
            final AprSession result = new AprSocketSession(this, processor, s);
            success = true;
            return result;
        } finally {
            if (!success) {
                Socket.close(s);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Long open(final SocketAddress localAddress) throws Exception {
        final InetSocketAddress la = (InetSocketAddress) localAddress;
        final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_SCTP, pool);

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

            // Configure the server socket,
            result = Socket.optSet(handle, Socket.APR_SO_REUSEADDR, isReuseAddress() ? 1 : 0);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }
            result = Socket.optSet(handle, Socket.APR_SO_RCVBUF, getSessionConfig().getReceiveBufferSize());
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }

            // and bind.
            long sa;
            if (la != null) {
                if (la.getAddress() == null) {
                    sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, la.getPort(), 0, pool);
                } else {
                    sa = Address.info(la.getAddress().getHostAddress(), Socket.APR_INET, la.getPort(), 0, pool);
                }
            } else {
                sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, 0, 0, pool);
            }

            result = Socket.bind(handle, sa);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }
            result = Socket.listen(handle, getBacklog());
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }

            result = Poll.add(pollset, handle, Poll.APR_POLLIN);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }
            success = true;
        } finally {
            if (!success) {
                close(handle);
            }
        }
        return handle;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init() throws Exception {
        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibrary.getInstance().getRootPool());

        wakeupSocket = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP, pool);

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
    protected SocketAddress localAddress(final Long handle) throws Exception {
        final long la = Address.get(Socket.APR_LOCAL, handle);
        return new InetSocketAddress(Address.getip(la), Address.getInfo(la).port);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int select() throws Exception {
        int rv = Poll.poll(pollset, Integer.MAX_VALUE, polledSockets, false);
        if (rv <= 0) {
            // We have had an error. It can simply be that we have reached
            // the timeout (very unlikely, as we have set it to MAX_INTEGER)
            if (rv != APR_TIMEUP_ERROR) {
                // It's not a timeout being exceeded. Throw the error
                throwException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    Poll.add(pollset, polledSockets[i], Poll.APR_POLLIN);
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

                if ((flag & Poll.APR_POLLIN) != 0) {
                    polledHandles.add(socket);
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
    protected void close(final Long handle) throws Exception {
        Poll.remove(pollset, handle);
        final int result = Socket.close(handle);
        if (result != Status.APR_SUCCESS) {
            throwException(result);
        }
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
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) super.getLocalAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InetSocketAddress getDefaultLocalAddress() {
        return (InetSocketAddress) super.getDefaultLocalAddress();
    }

    /**
     * @see #setDefaultLocalAddress(SocketAddress)
     * 
     * @param localAddress
     *            The localAddress to set
     */
    public void setDefaultLocalAddress(final InetSocketAddress localAddress) {
        super.setDefaultLocalAddress(localAddress);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TransportMetadata getTransportMetadata() {
        return AprSocketSession.METADATA;
    }

    /**
     * Convert an APR code into an Exception with the corresponding message
     *
     * @param code
     *            error number
     * @throws IOException
     *             the generated exception
     */
    private void throwException(final int code) throws IOException {
        throw new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

    @Override
    protected void init(final SelectorProvider selectorProvider) throws Exception {
        init();
    }
}
