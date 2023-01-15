package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Iterator;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.polling.AbstractPollingIoConnector;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.transport.socket.DatagramConnector;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.DefaultDatagramSessionConfig;
import org.apache.mina.transport.socket.nio.NioDatagramConnector;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Multicast;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * {@link IoConnector} for datagram transport (UDP/IP).
 *
 * @author The Apache MINA Project (dev@mina.apache.org)
 * @version $Rev: 706280 $, $Date: 2008-10-20 15:40:20 +0200 (Mon, 20 Oct 2008) $
 */
@ThreadSafe
public final class AprDatagramConnector extends AbstractPollingIoConnector<AprSession, Long>
        implements DatagramConnector {

    private static final int POLLSET_SIZE = 1024;

    private volatile long pool;
    private volatile long pollset; // socket poller
    private byte ttl = 2;
    private String iface = "";

    private InetSocketAddress remoteAddress;

    /**
     * Creates a new instance.
     */
    public AprDatagramConnector() {
        super(new DefaultDatagramSessionConfig(), AprDatagramIoProcessor.class);
    }

    /**
     * Creates a new instance.
     */
    public AprDatagramConnector(final int processorCount) {
        super(new DefaultDatagramSessionConfig(), AprDatagramIoProcessor.class, processorCount);
    }

    /**
     * Creates a new instance.
     */
    public AprDatagramConnector(final IoProcessor<AprSession> processor) {
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
     * @see org.apache.mina.core.service.SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int)
     * @since 2.0.0-M4
     */
    public AprDatagramConnector(final Class<? extends IoProcessor<AprSession>> processorClass,
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
     * @see org.apache.mina.core.service.SimpleIoProcessorPool#SimpleIoProcessorPool(Class, Executor, int)
     * @see org.apache.mina.core.service.SimpleIoProcessorPool#DEFAULT_SIZE
     * @since 2.0.0-M4
     */
    public AprDatagramConnector(final Class<? extends IoProcessor<AprSession>> processorClass) {
        super(new DefaultDatagramSessionConfig(), processorClass);
    }

    @Override
    public TransportMetadata getTransportMetadata() {
        return AprDatagramSession.METADATA;
    }

    @Override
    public DatagramSessionConfig getSessionConfig() {
        return (DatagramSessionConfig) sessionConfig;
    }

    @Override
    public InetSocketAddress getDefaultRemoteAddress() {
        return (InetSocketAddress) super.getDefaultRemoteAddress();
    }

    @Override
    public void setDefaultRemoteAddress(final InetSocketAddress defaultRemoteAddress) {
        super.setDefaultRemoteAddress(defaultRemoteAddress);
    }

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

            //set the multicast TTL for the new socket
            result = Multicast.hops(handle, ttl);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }

            //set the interface to use to send multicast datagrams
            if (!"".equals(iface)) {
                final long ifa = Address.info(iface, Socket.APR_INET, 0, 0, pool);

                result = Multicast.ointerface(handle, ifa);
                if (result != Status.APR_SUCCESS) {
                    throwException(result);
                }
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

    @Override
    protected boolean connect(final Long handle, final SocketAddress remoteAddress) throws Exception {
        final InetSocketAddress ra = (InetSocketAddress) remoteAddress;
        this.remoteAddress = ra;
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

    @Override
    protected AprDatagramSession newSession(final IoProcessor<AprSession> processor, final Long handle) {
        try {
            final AprDatagramSession newSession = new AprDatagramSession(this, processor, handle, remoteAddress);
            return newSession;
        } catch (final Exception e) {
            return null;
        }
    }

    @Override
    protected void close(final Long handle) throws Exception {
        finishConnect(handle);
        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }
    }

    // Unused extension points.
    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Long> allHandles() {
        return java.util.Collections.EMPTY_LIST.iterator();
    }

    @Override
    protected ConnectionRequest getConnectionRequest(final Long handle) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void destroy() throws Exception {}

    @Override
    protected boolean finishConnect(final Long handle) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void register(final Long handle, final ConnectionRequest request) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    protected int select(final int timeout) throws Exception {
        return 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Iterator<Long> selectedHandles() {
        return java.util.Collections.EMPTY_LIST.iterator();
    }

    @Override
    protected void wakeup() {}

    private void throwException(final int code) throws IOException {
        throw new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

    /**
     * Set the TTL for packets sent to multicast addresses. Connector must be rebound for changes to take effect.
     *
     * @param t
     *            The TTL value (0-255);
     * 
     */
    public void setTtl(final int t) {
        ttl = (byte) t;
    }

    /**
     * Sets the interface to be used for outgoing multicast message. Connector must be rebound for changes to take
     * effect.
     * 
     * @param ip
     *            The IP of the interface to be used in dotted decimal notation
     */
    public void setMulticastInterface(final String ip) {
        iface = ip;
    }

}
