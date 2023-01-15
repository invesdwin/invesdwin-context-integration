package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.buffer.IoBuffer;
import org.apache.mina.core.file.FileRegion;
import org.apache.mina.core.polling.AbstractPollingIoProcessor;
import org.apache.mina.core.session.SessionState;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.File;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

/**
 * The class in charge of processing socket level IO events for the {@link AprSocketConnector}
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@ThreadSafe
public final class AprDatagramIoProcessor extends AbstractPollingIoProcessor<AprSession> {
    private static final int POLLSET_SIZE = 1024;

    private final Map<Long, AprSession> allSessions = new HashMap<Long, AprSession>(POLLSET_SIZE);

    private final Object wakeupLock = new Object();

    private final long wakeupSocket;

    private volatile boolean toBeWakenUp;

    private final long pool;

    private final long bufferPool; // memory pool

    private final long pollset; // socket poller

    private final long[] polledSockets = new long[POLLSET_SIZE << 1];

    private final Queue<AprSession> polledSessions = new ConcurrentLinkedQueue<AprSession>();

    /**
     * Create a new instance of {@link AprDatagramIoProcessor} with a given Exector for handling I/Os events.
     *
     * @param executor
     *            the {@link Executor} for handling I/O events
     */
    public AprDatagramIoProcessor(final Executor executor) {
        super(executor);

        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibrary.getInstance().getRootPool());
        bufferPool = Pool.create(AprLibrary.getInstance().getRootPool());

        try {
            wakeupSocket = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP, pool);
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Error e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeIoException("Failed to create a wakeup socket.", e);
        }

        boolean success = false;
        long newPollset;
        try {
            newPollset = Poll.create(POLLSET_SIZE, pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);

            if (newPollset == 0) {
                newPollset = Poll.create(62, pool, Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);
            }

            pollset = newPollset;
            if (pollset < 0) {
                if (Status.APR_STATUS_IS_ENOTIMPL(-(int) pollset)) {
                    throw new RuntimeIoException("Thread-safe pollset is not supported in this platform.");
                }
            }
            success = true;
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Error e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeIoException("Failed to create a pollset.", e);
        } finally {
            if (!success) {
                dispose();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doDispose() {
        Poll.destroy(pollset);
        Socket.close(wakeupSocket);
        Pool.destroy(bufferPool);
        Pool.destroy(pool);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int select() throws Exception {
        return select(Integer.MAX_VALUE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int select(final long timeout) throws Exception {
        int rv = Poll.poll(pollset, 1000 * timeout, polledSockets, false);
        if (rv <= 0) {
            if (rv != -120001) {
                throwException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    final long socket = polledSockets[i];
                    final AprSession session = allSessions.get(socket);
                    if (session == null) {
                        continue;
                    }

                    final int flag = (session.isInterestedInRead() ? Poll.APR_POLLIN : 0)
                            | (session.isInterestedInWrite() ? Poll.APR_POLLOUT : 0);

                    Poll.add(pollset, socket, flag);
                }
            } else if (rv < 0) {
                throwException(rv);
            }

            return 0;
        } else {
            rv <<= 1;
            if (!polledSessions.isEmpty()) {
                polledSessions.clear();
            }
            for (int i = 0; i < rv; i++) {
                final long flag = polledSockets[i];
                final long socket = polledSockets[++i];
                if (socket == wakeupSocket) {
                    synchronized (wakeupLock) {
                        Poll.remove(pollset, wakeupSocket);
                        toBeWakenUp = false;
                        wakeupCalled.set(true);
                    }
                    continue;
                }
                final AprSession session = allSessions.get(socket);
                if (session == null) {
                    continue;
                }

                session.setReadable((flag & Poll.APR_POLLIN) != 0);
                session.setWritable((flag & Poll.APR_POLLOUT) != 0);

                polledSessions.add(session);
            }

            return polledSessions.size();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isSelectorEmpty() {
        return allSessions.isEmpty();
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
    protected Iterator<AprSession> allSessions() {
        return allSessions.values().iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int allSessionsCount() {
        return allSessions.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Iterator<AprSession> selectedSessions() {
        return polledSessions.iterator();
    }

    @Override
    protected void init(final AprSession session) throws Exception {
        final long s = session.getDescriptor();
        Socket.optSet(s, Socket.APR_SO_NONBLOCK, 1);
        Socket.timeoutSet(s, 0);

        final int rv = Poll.add(pollset, s, Poll.APR_POLLIN);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }

        session.setInterestedInRead(true);
        allSessions.put(s, session);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void destroy(final AprSession session) throws Exception {
        if (allSessions.remove(session.getDescriptor()) == null) {
            // Already destroyed.
            return;
        }

        int ret = Poll.remove(pollset, session.getDescriptor());
        try {
            if (ret != Status.APR_SUCCESS) {
                throwException(ret);
            }
        } finally {
            ret = Socket.close(session.getDescriptor());

            // destroying the session because it won't be reused
            // after this point
            Socket.destroy(session.getDescriptor());
            session.setDescriptor(0);

            if (ret != Status.APR_SUCCESS) {
                throwException(ret);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected SessionState getState(final AprSession session) {
        final long socket = session.getDescriptor();

        if (socket != 0) {
            return SessionState.OPENED;
        } else if (allSessions.get(socket) != null) {
            return SessionState.OPENING; // will occur ?
        } else {
            return SessionState.CLOSING;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isReadable(final AprSession session) {
        return session.isReadable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isWritable(final AprSession session) {
        return session.isWritable();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isInterestedInRead(final AprSession session) {
        return session.isInterestedInRead();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isInterestedInWrite(final AprSession session) {
        return session.isInterestedInWrite();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setInterestedInRead(final AprSession session, final boolean isInterested) throws Exception {
        if (session.isInterestedInRead() == isInterested) {
            return;
        }

        int rv = Poll.remove(pollset, session.getDescriptor());

        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }

        final int flags = (isInterested ? Poll.APR_POLLIN : 0) | (session.isInterestedInWrite() ? Poll.APR_POLLOUT : 0);

        rv = Poll.add(pollset, session.getDescriptor(), flags);

        if (rv == Status.APR_SUCCESS) {
            session.setInterestedInRead(isInterested);
        } else {
            throwException(rv);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setInterestedInWrite(final AprSession session, final boolean isInterested) throws Exception {
        if (session.isInterestedInWrite() == isInterested) {
            return;
        }

        int rv = Poll.remove(pollset, session.getDescriptor());

        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }

        final int flags = (session.isInterestedInRead() ? Poll.APR_POLLIN : 0) | (isInterested ? Poll.APR_POLLOUT : 0);

        rv = Poll.add(pollset, session.getDescriptor(), flags);

        if (rv == Status.APR_SUCCESS) {
            session.setInterestedInWrite(isInterested);
        } else {
            throwException(rv);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int read(final AprSession session, final IoBuffer buffer) throws Exception {
        int bytes;
        final int capacity = buffer.remaining();
        // Using Socket.recv() directly causes memory leak. :-(
        final java.nio.ByteBuffer b = Pool.alloc(bufferPool, capacity);

        try {
            bytes = Socket.recvb(session.getDescriptor(), b, 0, capacity);

            if (bytes > 0) {
                b.position(0);
                b.limit(bytes);
                buffer.put(b);
            } else if (bytes < 0) {
                if (Status.APR_STATUS_IS_EOF(-bytes)) {
                    bytes = -1;
                } else if (Status.APR_STATUS_IS_EAGAIN(-bytes)) {
                    bytes = 0;
                } else {
                    throwException(bytes);
                }
            }
        } finally {
            Pool.clear(bufferPool);
        }

        return bytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int write(final AprSession session, final IoBuffer buf, final int length) throws IOException {
        int writtenBytes;
        if (buf.isDirect()) {
            writtenBytes = Socket.sendb(session.getDescriptor(), buf.buf(), buf.position(), length);
        } else {
            writtenBytes = Socket.send(session.getDescriptor(), buf.array(), buf.position(), length);
            if (writtenBytes > 0) {
                buf.skip(writtenBytes);
            }
        }

        if (writtenBytes < 0) {
            if (Status.APR_STATUS_IS_EAGAIN(-writtenBytes)) {
                writtenBytes = 0;
            } else if (Status.APR_STATUS_IS_EOF(-writtenBytes)) {
                writtenBytes = 0;
            } else {
                throwException(writtenBytes);
            }
        }
        return writtenBytes;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int transferFile(final AprSession session, final FileRegion region, final int length) throws Exception {
        if (region.getFilename() == null) {
            throw new UnsupportedOperationException();
        }

        final long fd = File.open(region.getFilename(),
                File.APR_FOPEN_READ | File.APR_FOPEN_SENDFILE_ENABLED | File.APR_FOPEN_BINARY, 0,
                Socket.pool(session.getDescriptor()));
        final long numWritten = Socket.sendfilen(session.getDescriptor(), fd, region.getPosition(), length, 0);
        File.close(fd);

        if (numWritten < 0) {
            if (numWritten == -Status.EAGAIN) {
                return 0;
            }
            throw new IOException(
                    org.apache.tomcat.jni.Error.strerror((int) -numWritten) + " (code: " + numWritten + ")");
        }
        return (int) numWritten;
    }

    private void throwException(final int code) throws IOException {
        throw new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void registerNewSelector() {
        // Do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isBrokenConnection() throws IOException {
        // Here, we assume that this is the case.
        return true;
    }

    /**
     * Receive data and retrieve the address and port of the sender. NOTE: This does not return the correct port under
     * WIN32, the default value used when creating the remote address structure is not overwritten. Works under Linux
     * though...
     * 
     * @param handle
     *            the socket from which to receive.
     * @param buffer
     *            the buffer that is to hold the data.
     */
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

    protected int send(final AprSession session, final IoBuffer buffer, final SocketAddress remoteAddress)
            throws Exception {

        final InetSocketAddress rem = (InetSocketAddress) remoteAddress;

        final long ra = Address.info(rem.getAddress().getHostAddress(), Socket.APR_INET, rem.getPort(), 0, pool);

        return Socket.sendto(session.getDescriptor(), ra, 0, buffer.array(), 0, buffer.limit());
    }
}
