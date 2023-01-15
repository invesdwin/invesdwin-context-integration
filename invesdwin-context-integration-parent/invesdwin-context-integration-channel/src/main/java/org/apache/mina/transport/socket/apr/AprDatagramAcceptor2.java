package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.service.AbstractIoAcceptor;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.core.session.AbstractIoSession;
import org.apache.mina.core.session.ExpiringSessionRecycler;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.core.session.IoSessionConfig;
import org.apache.mina.core.session.IoSessionRecycler;
import org.apache.mina.core.write.WriteRequest;
import org.apache.mina.core.write.WriteRequestQueue;
import org.apache.mina.transport.socket.DatagramAcceptor;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.DefaultDatagramSessionConfig;
import org.apache.mina.util.ExceptionMonitor;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * {@link IoAcceptor} for datagram transport (UDP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 * @org.apache.xbean.XBean
 */
@ThreadSafe
public final class AprDatagramAcceptor2 extends AbstractIoAcceptor
        implements DatagramAcceptor, IoProcessor<AprSession> {
    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;

    private static final int POLLSET_SIZE = 1024;
    /**
     * A session recycler that is used to retrieve an existing session, unless it's too old.
     **/
    private static final IoSessionRecycler DEFAULT_RECYCLER = new ExpiringSessionRecycler();

    /**
     * A timeout used for the select, as we need to get out to deal with idle sessions
     */
    private static final long SELECT_TIMEOUT = 1000L;

    /** A lock used to protect the selector to be waked up before it's created */
    private final Semaphore lock = new Semaphore(1);

    /** A queue used to store the list of pending Binds */
    private final Queue<AcceptorOperationFuture> registerQueue = new ConcurrentLinkedQueue<>();

    private final Queue<AcceptorOperationFuture> cancelQueue = new ConcurrentLinkedQueue<>();

    private final Queue<AprSession> flushingSessions = new ConcurrentLinkedQueue<>();

    private final Map<SocketAddress, Long> boundHandles = java.util.Collections
            .synchronizedMap(new HashMap<SocketAddress, Long>());

    private IoSessionRecycler sessionRecycler = DEFAULT_RECYCLER;

    private final ServiceOperationFuture disposalFuture = new ServiceOperationFuture();

    private volatile boolean selectable;

    /** The thread responsible of accepting incoming requests */
    private Acceptor acceptor;

    private long lastIdleCheckTime;

    private final Object wakeupLock = new Object();

    private volatile long wakeupSocket;

    private volatile boolean toBeWakenUp;

    private volatile long pool;

    private volatile long pollset; // socket poller

    private final long[] polledSockets = new long[POLLSET_SIZE << 1];

    private final Queue<Long> polledHandles = new ConcurrentLinkedQueue<Long>();

    private boolean reuseAddress = false;

    /**
     * Define the number of socket that can wait to be accepted. Default to 50 (as in the SocketServer default).
     */
    private int backlog = 50;

    /**
     * Creates a new instance.
     */
    public AprDatagramAcceptor2() {
        this(new DefaultDatagramSessionConfig(), null);
    }

    /**
     * Creates a new instance.
     * 
     * @param executor
     *            The executor to use
     */
    public AprDatagramAcceptor2(final Executor executor) {
        this(new DefaultDatagramSessionConfig(), executor);
    }

    /**
     * Creates a new instance.
     */
    private AprDatagramAcceptor2(final IoSessionConfig sessionConfig, final Executor executor) {
        super(sessionConfig, executor);

        try {
            init();
            selectable = true;
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeIoException("Failed to initialize.", e);
        } finally {
            if (!selectable) {
                try {
                    destroy();
                } catch (final Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);
                }
            }
        }
    }

    protected Iterator<Long> selectedHandles() {
        return polledHandles.iterator();
    }

    /**
     * This private class is used to accept incoming connection from clients. It's an infinite loop, which can be
     * stopped when all the registered handles have been removed (unbound).
     */
    private class Acceptor implements Runnable {
        @Override
        public void run() {
            int nHandles = 0;
            lastIdleCheckTime = System.currentTimeMillis();

            // Release the lock
            lock.release();

            while (selectable) {
                try {
                    final int selected = select(SELECT_TIMEOUT);

                    nHandles += registerHandles();

                    if (nHandles == 0) {
                        try {
                            lock.acquire();

                            if (registerQueue.isEmpty() && cancelQueue.isEmpty()) {
                                acceptor = null;
                                break;
                            }
                        } finally {
                            lock.release();
                        }
                    }

                    if (selected > 0) {
                        processReadySessions(selectedHandles());
                    }

                    final long currentTime = System.currentTimeMillis();
                    flushSessions(currentTime);
                    nHandles -= unregisterHandles();

                    notifyIdleSessions(currentTime);
                } catch (final ClosedSelectorException cse) {
                    // If the selector has been closed, we can exit the loop
                    ExceptionMonitor.getInstance().exceptionCaught(cse);
                    break;
                } catch (final Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);

                    try {
                        Thread.sleep(1000);
                    } catch (final InterruptedException e1) {
                    }
                }
            }

            if (selectable && isDisposing()) {
                selectable = false;
                try {
                    destroy();
                } catch (final Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);
                } finally {
                    disposalFuture.setValue(true);
                }
            }
        }
    }

    private int registerHandles() {
        while (true) {
            final AcceptorOperationFuture req = registerQueue.poll();

            if (req == null) {
                break;
            }

            final Map<SocketAddress, Long> newHandles = new HashMap<>();
            final List<SocketAddress> localAddresses = req.getLocalAddresses();

            try {
                for (final SocketAddress socketAddress : localAddresses) {
                    final Long handle = open(socketAddress);
                    newHandles.put(localAddress(handle), handle);
                }

                boundHandles.putAll(newHandles);

                getListeners().fireServiceActivated();
                req.setDone();

                return newHandles.size();
            } catch (final Exception e) {
                req.setException(e);
            } finally {
                // Roll back if failed to bind all addresses.
                if (req.getException() != null) {
                    for (final Long handle : newHandles.values()) {
                        try {
                            close(handle);
                        } catch (final Exception e) {
                            ExceptionMonitor.getInstance().exceptionCaught(e);
                        }
                    }

                    wakeup();
                }
            }
        }

        return 0;
    }

    private void processReadySessions(final Iterator<Long> handles) {
        while (handles.hasNext()) {
            try {
                final Long handle = handles.next();

                readHandle(handle);

                for (final IoSession session : getManagedSessions().values()) {
                    final AprDatagramSession x = (AprDatagramSession) session;
                    if (x.getDescriptor() == handle.longValue()) {
                        scheduleFlush(x);
                    }
                }

            } catch (final Exception e) {
                ExceptionMonitor.getInstance().exceptionCaught(e);
            } finally {
                handles.remove();
            }
        }
    }

    private boolean scheduleFlush(final AprSession session) {
        // Set the schedule for flush flag if the session
        // has not already be added to the flushingSessions
        // queue
        if (session.setScheduledForFlush(true)) {
            flushingSessions.add(session);
            return true;
        } else {
            return false;
        }
    }

    private void readHandle(final Long handle) throws Exception {
        final IoBuffer readBuf = IoBuffer.allocate(getSessionConfig().getReadBufferSize());

        final SocketAddress remoteAddress = receive(handle, readBuf);

        if (remoteAddress != null) {
            final IoSession session = newSessionWithoutLock(remoteAddress, localAddress(handle));

            readBuf.flip();

            if (!session.isReadSuspended()) {
                session.getFilterChain().fireMessageReceived(readBuf);
            }
        }
    }

    private IoSession newSessionWithoutLock(final SocketAddress remoteAddress, final SocketAddress localAddress)
            throws Exception {
        final Long handle = boundHandles.get(localAddress);

        if (handle == null) {
            throw new IllegalArgumentException("Unknown local address: " + localAddress);
        }

        IoSession session;

        synchronized (sessionRecycler) {
            session = sessionRecycler.recycle(remoteAddress);

            if (session != null) {
                return session;
            }

            // If a new session needs to be created.
            final AprDatagramSession newSession = newSession(this, handle, remoteAddress);
            getSessionRecycler().put(newSession);
            session = newSession;
        }

        initSession(session, null, null);

        try {
            this.getFilterChainBuilder().buildFilterChain(session.getFilterChain());
            getListeners().fireSessionCreated(session);
        } catch (final Exception e) {
            ExceptionMonitor.getInstance().exceptionCaught(e);
        }

        return session;
    }

    private void flushSessions(final long currentTime) {
        while (true) {
            final AprSession session = flushingSessions.poll();

            if (session == null) {
                break;
            }

            // Reset the Schedule for flush flag for this session,
            // as we are flushing it now
            session.unscheduledForFlush();

            try {
                final boolean flushedAll = flush(session, currentTime);

                if (flushedAll && !session.getWriteRequestQueue().isEmpty(session) && !session.isScheduledForFlush()) {
                    scheduleFlush(session);
                }
            } catch (final Exception e) {
                session.getFilterChain().fireExceptionCaught(e);
            }
        }
    }

    private boolean flush(final AprSession session, final long currentTime) throws Exception {
        final WriteRequestQueue writeRequestQueue = session.getWriteRequestQueue();
        final int maxWrittenBytes = session.getConfig().getMaxReadBufferSize()
                + (session.getConfig().getMaxReadBufferSize() >>> 1);

        int writtenBytes = 0;

        try {
            while (true) {
                WriteRequest req = session.getCurrentWriteRequest();

                if (req == null) {
                    req = writeRequestQueue.poll(session);

                    if (req == null) {
                        setInterestedInWrite(session, false);
                        break;
                    }

                    session.setCurrentWriteRequest(req);
                }

                final IoBuffer buf = (IoBuffer) req.getMessage();

                if (buf.remaining() == 0) {
                    // Clear and fire event
                    session.setCurrentWriteRequest(null);
                    buf.reset();
                    session.getFilterChain().fireMessageSent(req);
                    continue;
                }

                SocketAddress destination = req.getDestination();

                if (destination == null) {
                    destination = session.getRemoteAddress();
                }

                final int localWrittenBytes = send(session, buf, destination);

                if ((localWrittenBytes == 0) || (writtenBytes >= maxWrittenBytes)) {
                    // Kernel buffer is full or wrote too much
                    setInterestedInWrite(session, true);

                    return false;
                } else {
                    setInterestedInWrite(session, false);

                    // Clear and fire event
                    session.setCurrentWriteRequest(null);
                    writtenBytes += localWrittenBytes;
                    buf.reset();
                    session.getFilterChain().fireMessageSent(req);
                }
            }
        } finally {
            session.increaseWrittenBytes(writtenBytes, currentTime);
        }

        return true;
    }

    private int unregisterHandles() {
        int nHandles = 0;

        while (true) {
            final AcceptorOperationFuture request = cancelQueue.poll();
            if (request == null) {
                break;
            }

            // close the channels
            for (final SocketAddress socketAddress : request.getLocalAddresses()) {
                final Long handle = boundHandles.remove(socketAddress);

                if (handle == null) {
                    continue;
                }

                try {
                    close(handle);
                    wakeup(); // wake up again to trigger thread death
                } catch (final Exception e) {
                    ExceptionMonitor.getInstance().exceptionCaught(e);
                } finally {
                    nHandles++;
                }
            }

            request.setDone();
        }

        return nHandles;
    }

    private void notifyIdleSessions(final long currentTime) {
        // process idle sessions
        if (currentTime - lastIdleCheckTime >= 1000) {
            lastIdleCheckTime = currentTime;
            AbstractIoSession.notifyIdleness(getListeners().getManagedSessions().values().iterator(), currentTime);
        }
    }

    /**
     * Starts the inner Acceptor thread.
     */
    private void startupAcceptor() throws InterruptedException {
        if (!selectable) {
            registerQueue.clear();
            cancelQueue.clear();
            flushingSessions.clear();
        }

        lock.acquire();

        if (acceptor == null) {
            acceptor = new Acceptor();
            executeWorker(acceptor);
        } else {
            lock.release();
        }
    }

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
    public void add(final AprSession session) {
        // Nothing to do for UDP
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Set<SocketAddress> bindInternal(final List<? extends SocketAddress> localAddresses) throws Exception {
        // Create a bind request as a Future operation. When the selector
        // have handled the registration, it will signal this future.
        final AcceptorOperationFuture request = new AcceptorOperationFuture(localAddresses);

        // adds the Registration request to the queue for the Workers
        // to handle
        registerQueue.add(request);

        // creates the Acceptor instance and has the local
        // executor kick it off.
        startupAcceptor();

        // As we just started the acceptor, we have to unblock the select()
        // in order to process the bind request we just have added to the
        // registerQueue.
        try {
            lock.acquire();

            // Wait a bit to give a chance to the Acceptor thread to do the select()
            Thread.sleep(10);
            wakeup();
        } finally {
            lock.release();
        }

        // Now, we wait until this request is completed.
        request.awaitUninterruptibly();

        if (request.getException() != null) {
            throw request.getException();
        }

        // Update the local addresses.
        // setLocalAddresses() shouldn't be called from the worker thread
        // because of deadlock.
        final Set<SocketAddress> newLocalAddresses = new HashSet<>();

        for (final Long handle : boundHandles.values()) {
            newLocalAddresses.add(localAddress(handle));
        }

        return newLocalAddresses;
    }

    protected void close(final Long handle) throws Exception {
        Poll.remove(pollset, handle);
        final int result = Socket.close(handle);
        if (result != Status.APR_SUCCESS) {
            throwException(result);
        }
    }

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
    protected void dispose0() throws Exception {
        unbind();
        startupAcceptor();
        wakeup();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void flush(final AprSession session) {
        if (scheduleFlush(session)) {
            wakeup();
        }
    }

    @Override
    public InetSocketAddress getDefaultLocalAddress() {
        return (InetSocketAddress) super.getDefaultLocalAddress();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) super.getLocalAddress();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DatagramSessionConfig getSessionConfig() {
        return (DatagramSessionConfig) sessionConfig;
    }

    @Override
    public IoSessionRecycler getSessionRecycler() {
        return sessionRecycler;
    }

    @Override
    public TransportMetadata getTransportMetadata() {
        return AprDatagramSession.METADATA;
    }

    protected boolean isReadable(final Long handle) {
        return true;
    }

    protected boolean isWritable(final Long handle) {
        return true;
    }

    protected SocketAddress localAddress(final Long handle) throws Exception {
        final long la = Address.get(Socket.APR_LOCAL, handle);
        return new InetSocketAddress(Address.getip(la), Address.getInfo(la).port);
    }

    protected AprDatagramSession newSession(final IoProcessor<AprSession> processor, final Long handle,
            final SocketAddress remoteAddress) throws Exception {
        final AprDatagramSession newSession = new AprDatagramSession(this, processor, handle,
                (InetSocketAddress) remoteAddress);
        return newSession;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IoSession newSession(final SocketAddress remoteAddress, final SocketAddress localAddress) {
        if (isDisposing()) {
            throw new IllegalStateException("The Acceptor is being disposed.");
        }

        if (remoteAddress == null) {
            throw new IllegalArgumentException("remoteAddress");
        }

        synchronized (bindLock) {
            if (!isActive()) {
                throw new IllegalStateException("Can't create a session from a unbound service.");
            }

            try {
                return newSessionWithoutLock(remoteAddress, localAddress);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (final Exception e) {
                throw new RuntimeIoException("Failed to create a session.", e);
            }
        }
    }

    /**
     * @return the flag that sets the reuseAddress information
     */
    public boolean isReuseAddress() {
        return reuseAddress;
    }

    /**
     * Set the Reuse Address flag
     * 
     * @param reuseAddress
     *            The flag to set
     */
    public void setReuseAddress(final boolean reuseAddress) {
        synchronized (bindLock) {
            if (isActive()) {
                throw new IllegalStateException("backlog can't be set while the acceptor is bound.");
            }

            this.reuseAddress = reuseAddress;
        }
    }

    //CHECKSTYLE:OFF
    protected Long open(final SocketAddress localAddress) throws Exception {
        //CHECKSTYLE:ON
        final InetSocketAddress la = (InetSocketAddress) localAddress;
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
            final long sa;
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
     * @return the backLog
     */
    public int getBacklog() {
        return backlog;
    }

    /**
     * Sets the Backlog parameter
     * 
     * @param backlog
     *            the backlog variable
     */
    public void setBacklog(final int backlog) {
        synchronized (bindLock) {
            if (isActive()) {
                throw new IllegalStateException("backlog can't be set while the acceptor is bound.");
            }

            this.backlog = backlog;
        }
    }

    protected SocketAddress receive(final Long handle, final IoBuffer buffer) throws Exception {
        /*
         * initialize a new byte array for now. Is the data correctly transferred to the MINA framework like this? There
         * must be some way to write directly to buffer...
         */
        final byte[] b = new byte[buffer.capacity()];
        //initialize a new apr_sockaddr_t
        final long ra = Address.info("123.123.123.123", Socket.APR_INET, 0, 0, pool);
        //read the datagram and fill in the apr_sockaddr_t with the remote address
        final int r = Socket.recvfrom(ra, handle, 0, b, 0, buffer.capacity());
        //move the acquired data to the buffer used in the future. Performance problem, there must be a direct way to do this
        buffer.put(b, 0, r);
        //fill in the data to return (remote address)
        final SocketAddress sa = new InetSocketAddress(Address.getip(ra), Address.getInfo(ra).port);

        return sa;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove(final AprSession session) {
        getSessionRecycler().remove(session);
        getListeners().fireSessionDestroyed(session);
    }

    protected int select(final long timeout) throws Exception {
        int rv = Poll.poll(pollset, timeout * 1000, polledSockets, false);
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

    protected int send(final AprSession session, final IoBuffer buffer, final SocketAddress remoteAddress)
            throws Exception {
        final InetSocketAddress rem = (InetSocketAddress) remoteAddress;
        final long ra = Address.info(rem.getAddress().getHostAddress(), Socket.APR_INET, rem.getPort(), 0, pool);
        return Socket.sendto(session.getDescriptor(), ra, 0, buffer.array(), 0, buffer.limit());
    }

    @Override
    public void setDefaultLocalAddress(final InetSocketAddress localAddress) {
        setDefaultLocalAddress((SocketAddress) localAddress);
    }

    protected void setInterestedInWrite(final AprSession session, final boolean isInterested) throws Exception {
        session.setInterestedInWrite(isInterested);
    }

    @Override
    public void setSessionRecycler(final IoSessionRecycler sessionRecycler) {
        synchronized (bindLock) {
            if (isActive()) {
                throw new IllegalStateException("sessionRecycler can't be set while the acceptor is bound.");
            }

            if (sessionRecycler == null) {
                this.sessionRecycler = DEFAULT_RECYCLER;
            } else {
                this.sessionRecycler = sessionRecycler;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void unbind0(final List<? extends SocketAddress> localAddresses) throws Exception {
        final AcceptorOperationFuture request = new AcceptorOperationFuture(localAddresses);

        cancelQueue.add(request);
        startupAcceptor();
        wakeup();

        request.awaitUninterruptibly();

        if (request.getException() != null) {
            throw request.getException();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateTrafficControl(final AprSession session) {
        // Nothing to do
    }

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
    public void write(final AprSession session, final WriteRequest pWriteRequest) {
        WriteRequest writeRequest = pWriteRequest;

        // We will try to write the message directly
        final long currentTime = System.currentTimeMillis();
        final WriteRequestQueue writeRequestQueue = session.getWriteRequestQueue();
        final int maxWrittenBytes = session.getConfig().getMaxReadBufferSize()
                + (session.getConfig().getMaxReadBufferSize() >>> 1);

        int writtenBytes = 0;

        // Deal with the special case of a Message marker (no bytes in the request)
        // We just have to return after having calle dthe messageSent event
        IoBuffer buf = (IoBuffer) writeRequest.getMessage();

        if (buf.remaining() == 0) {
            // Clear and fire event
            session.setCurrentWriteRequest(null);
            buf.reset();
            session.getFilterChain().fireMessageSent(writeRequest);
            return;
        }

        // Now, write the data
        try {
            while (true) {
                if (writeRequest == null) {
                    writeRequest = writeRequestQueue.poll(session);

                    if (writeRequest == null) {
                        setInterestedInWrite(session, false);
                        break;
                    }

                    session.setCurrentWriteRequest(writeRequest);
                }

                buf = (IoBuffer) writeRequest.getMessage();

                if (buf.remaining() == 0) {
                    // Clear and fire event
                    session.setCurrentWriteRequest(null);
                    session.getFilterChain().fireMessageSent(writeRequest);
                    continue;
                }

                SocketAddress destination = writeRequest.getDestination();

                if (destination == null) {
                    destination = session.getRemoteAddress();
                }

                final int localWrittenBytes = send(session, buf, destination);

                if ((localWrittenBytes == 0) || (writtenBytes >= maxWrittenBytes)) {
                    // Kernel buffer is full or wrote too much
                    setInterestedInWrite(session, true);

                    session.getWriteRequestQueue().offer(session, writeRequest);
                    scheduleFlush(session);
                } else {
                    setInterestedInWrite(session, false);

                    // Clear and fire event
                    session.setCurrentWriteRequest(null);
                    writtenBytes += localWrittenBytes;
                    session.getFilterChain().fireMessageSent(writeRequest);

                    break;
                }
            }
        } catch (final Exception e) {
            session.getFilterChain().fireExceptionCaught(e);
        } finally {
            session.increaseWrittenBytes(writtenBytes, currentTime);
        }
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
}