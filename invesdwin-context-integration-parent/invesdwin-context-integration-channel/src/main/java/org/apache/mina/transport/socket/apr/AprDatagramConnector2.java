package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.polling.AbstractPollingIoConnector;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.SimpleIoProcessorPool;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.transport.socket.DatagramConnector;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.DefaultDatagramSessionConfig;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * {@link IoConnector} for datagram transport (UDP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@ThreadSafe
public final class AprDatagramConnector2 extends AbstractPollingIoConnector<AprSession, Long>
        implements DatagramConnector {

    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;

    private static final int POLLSET_SIZE = 1024;

    private final Map<Long, ConnectionRequest> requests = new HashMap<Long, ConnectionRequest>(POLLSET_SIZE);

    private volatile long pool;

    private volatile long pollset; // socket poller

    /**
     * Creates a new instance.
     */
    public AprDatagramConnector2() {
        super(new DefaultDatagramSessionConfig(), AprDatagramIoProcessor.class);
    }

    /**
     * Creates a new instance.
     * 
     * @param processorCount
     *            The number of IoProcessor instance to create
     */
    public AprDatagramConnector2(final int processorCount) {
        super(new DefaultDatagramSessionConfig(), AprDatagramIoProcessor.class, processorCount);
    }

    /**
     * Creates a new instance.
     * 
     * @param processor
     *            The IoProcessor instance to use
     */
    public AprDatagramConnector2(final IoProcessor<AprSession> processor) {
        super(new DefaultDatagramSessionConfig(), processor);
    }

    /**
     * Constructor for {@link NioDatagramConnector} with default configuration which will use a built-in thread pool
     * executor to manage the given number of processor instances. The processor class must have a constructor that
     * accepts ExecutorService or Executor as its single argument, or, failing that, a no-arg constructor.
     * 
     * @param processorClass
     *            the processor class.
     * @param processorCount
     *            the number of processors to instantiate.
     * @see SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int, java.nio.channels.spi.SelectorProvider)
     * @since 2.0.0-M4
     */
    public AprDatagramConnector2(final Class<? extends IoProcessor<AprSession>> processorClass,
            final int processorCount) {
        super(new DefaultDatagramSessionConfig(), processorClass, processorCount);
    }

    /**
     * Constructor for {@link NioDatagramConnector} with default configuration with default configuration which will use
     * a built-in thread pool executor to manage the default number of processor instances. The processor class must
     * have a constructor that accepts ExecutorService or Executor as its single argument, or, failing that, a no-arg
     * constructor. The default number of instances is equal to the number of processor cores in the system, plus one.
     * 
     * @param processorClass
     *            the processor class.
     * @see SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int, java.nio.channels.spi.SelectorProvider)
     * @since 2.0.0-M4
     */
    public AprDatagramConnector2(final Class<? extends IoProcessor<AprSession>> processorClass) {
        super(new DefaultDatagramSessionConfig(), processorClass);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public TransportMetadata getTransportMetadata() {
        return AprDatagramSession.METADATA;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatagramSessionConfig getSessionConfig() {
        return (DatagramSessionConfig) sessionConfig;
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
     * {@inheritDoc}
     */
    @Override
    protected void init() throws Exception {
        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibrary.getInstance().getRootPool());

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
    protected AprDatagramSession newSession(final IoProcessor<AprSession> processor, final Long handle)
            throws Exception {
        final long ra = Address.get(Socket.APR_REMOTE, handle);

        final InetSocketAddress remoteAddress = new InetSocketAddress(Address.getip(ra), Address.getInfo(ra).port);

        final AprDatagramSession session = new AprDatagramSession(this, processor, handle, remoteAddress);
        session.getConfig().setAll(getSessionConfig());
        return session;
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
    // Unused extension points.
    @Override
    protected Iterator<Long> allHandles() {
        return java.util.Collections.emptyIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ConnectionRequest getConnectionRequest(final Long handle) {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy() throws Exception {
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
    protected boolean finishConnect(final Long handle) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void register(final Long handle, final ConnectionRequest request) throws Exception {
        throw new UnsupportedOperationException();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int select(final int timeout) throws Exception {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<Long> selectedHandles() {
        return java.util.Collections.emptyIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void wakeup() {
        // Do nothing
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
