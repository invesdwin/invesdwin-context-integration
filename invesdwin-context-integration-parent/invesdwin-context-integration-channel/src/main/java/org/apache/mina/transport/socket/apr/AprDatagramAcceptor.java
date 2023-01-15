package org.apache.mina.transport.socket.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.annotation.concurrent.ThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.service.IoProcessor;
import org.apache.mina.core.service.TransportMetadata;
import org.apache.mina.core.session.ExpiringSessionRecycler;
import org.apache.mina.core.session.IoSessionRecycler;
import org.apache.mina.transport.socket.DatagramAcceptor;
import org.apache.mina.transport.socket.DatagramSessionConfig;
import org.apache.mina.transport.socket.DefaultDatagramSessionConfig;
import org.apache.mina.util.CircularQueue;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Multicast;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

@ThreadSafe
public class AprDatagramAcceptor extends APollingConnectionlessIoAcceptor<AprSession, Long>
        implements DatagramAcceptor {

    /**
     * A session recycler that is used to retrieve an existing session, unless it's too old.
     **/
    private static final IoSessionRecycler DEFAULT_RECYCLER = new ExpiringSessionRecycler();

    private static final int APR_TIMEUP_ERROR = -120001;
    private static final int POLLSET_SIZE = 1024;

    private final Object groupLock = new Object();
    private final Object wakeupLock = new Object();
    private volatile long wakeupSocket;
    private volatile boolean toBeWakenUp;
    private volatile long pool;
    private volatile long pollset; // socket poller
    private final long[] polledSockets = new long[POLLSET_SIZE << 1];
    private final List<Long> polledHandles = new CircularQueue<Long>(POLLSET_SIZE);
    private final Set<Long> failedHandles = new HashSet<Long>(POLLSET_SIZE);
    //List of groups this acceptor should belong to...
    private final List<String> groups = new LinkedList<String>();

    private IoSessionRecycler sessionRecycler = DEFAULT_RECYCLER;

    public AprDatagramAcceptor() {
        super(new DefaultDatagramSessionConfig(), AprDatagramIoProcessor.class);
    }

    public AprDatagramAcceptor(final Executor executor) {
        super(new DefaultDatagramSessionConfig(), executor, new AprDatagramIoProcessor(executor));
    }

    /**
     * Initialize all memory structures required
     * 
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
     * free all memory structures allocated for this acceptor
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

    @Override
    public TransportMetadata getTransportMetadata() {
        return AprDatagramSession.METADATA;
    }

    @Override
    public DatagramSessionConfig getSessionConfig() {
        return super.getSessionConfig();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) super.getLocalAddress();
    }

    @Override
    public InetSocketAddress getDefaultLocalAddress() {
        return (InetSocketAddress) super.getDefaultLocalAddress();
    }

    @Override
    public void setDefaultLocalAddress(final InetSocketAddress localAddress) {
        setDefaultLocalAddress((SocketAddress) localAddress);
    }

    //CHECKSTYLE:OFF
    @Override
    protected Long open(final SocketAddress localAddress) throws Exception {
        //CHECKSTYLE:ON
        final InetSocketAddress la = (InetSocketAddress) localAddress;
        final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP, pool);
        //Join all initially set multicast groups
        if (!groups.isEmpty()) {
            long ra;
            String[] gp = new String[groups.size()];
            gp = groups.toArray(gp);
            for (int i = 0; i < gp.length; i++) {
                ra = Address.info(gp[i], 0, 0, 0, pool);
                Multicast.join(handle, ra, 0, 0);
            }
        }
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
            result = Socket.optSet(handle, Socket.APR_SO_REUSEADDR, 1);
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

    //no real idea what this is supposed to do...
    protected boolean isReadable(final Long handle) {
        int rv = Poll.poll(pollset, 1000, polledSockets, false);
        if (rv <= 0) {
            return false;

        } else {
            rv <<= 1;
            for (int i = 0; i < rv; i++) {
                final long flag = polledSockets[i];
                final long socket = polledSockets[++i];
                if ((flag & Poll.APR_POLLIN) != 0 && socket == handle) {
                    return true;
                }
            }
            return false;
        }
    }

    //  no real idea what this is supposed to do...
    protected boolean isWritable(final Long handle) {
        int rv = Poll.poll(pollset, 1000, polledSockets, false);
        if (rv <= 0) {
            return false;

        } else {
            rv <<= 1;

            for (int i = 0; i < rv; i++) {
                final long flag = polledSockets[i];
                final long socket = polledSockets[++i];
                if ((flag & Poll.APR_POLLOUT) != 0 && socket == handle) {
                    return true;
                }
            }
            return false;
        }
    }

    protected AprDatagramSession newSession(final IoProcessor<AprSession> processor, final Long handle,
            final InetSocketAddress remoteAddress) {
        try {
            final AprDatagramSession newSession = new AprDatagramSession(this, processor, handle, remoteAddress);
            return newSession;
        } catch (final Exception e) {
            return null;
        }
    }

    private void throwException(final int code) throws IOException {
        throw new IOException(org.apache.tomcat.jni.Error.strerror(-code) + " (code: " + code + ")");
    }

    @Override
    protected void close(final Long handle) throws Exception {

        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }

    }

    @Override
    protected SocketAddress localAddress(final Long handle) throws Exception {
        final long la = Address.get(Socket.APR_LOCAL, handle);
        final SocketAddress sa = new InetSocketAddress(Address.getip(la), Address.getInfo(la).port);
        return sa;
    }

    @Override
    protected Iterator<Long> selectedHandles() {
        return polledHandles.iterator();
    }

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
     * Adds the acceptor to the specified multicast group. Currently Acceptor must be rebound for changes to take
     * effect.
     *
     * @param group
     *            The group to be joined, an IP address in dot-notation.
     * 
     */
    public void joinGroup(final String group) {
        //add the group to the multicast groups this acceptor has joined
        synchronized (groupLock) {
            if (!groups.contains(group)) {
                groups.add(group);
            }
        }
        //go through the registered sockets and have them all join the group
        //is this necessary for a DatagramAcceptor?
        final long[] ps = new long[POLLSET_SIZE << 1];
        int rv = Poll.poll(pollset, 1000, ps, false);
        if (rv <= 0) {
            return;

        } else {
            rv <<= 1;
            for (int i = 0; i < rv; i++) {
                //long flag = ps[i];
                final long socket = ps[++i];
                if (socket != wakeupSocket) {
                    try {
                        //if it's not the internal wakeup socket, join the group
                        final long ra = Address.info(group, 0, 0, 0, pool);
                        Multicast.join(socket, ra, 0, 0);
                    } catch (final Exception e) {

                    }
                }
            }
            return;
        }
    }

    /**
     * Removes the acceptor from the specified multicast group. Currently Acceptor must be rebound for changes to take
     * effect.
     *
     * @param group
     *            The group to be left, an IP address in dot-notation.
     * 
     */
    public void leaveGroup(final String group) {
        //remove the group from the list of multicast groups
        synchronized (groupLock) {
            groups.remove(group);
        }
        //go through the list of registered sockets and leave the group for each socket
        final long[] ps = new long[POLLSET_SIZE << 1];
        int rv = Poll.poll(pollset, 1000, ps, false);
        if (rv <= 0) {
            return;

        } else {
            rv <<= 1;
            for (int i = 0; i < rv; i++) {
                //long flag = ps[i];
                final long socket = ps[++i];
                if (socket != wakeupSocket) {
                    //if it is not the internal wakeup socket, leave the group
                    try {
                        final long ra = Address.info(group, 0, 0, 0, pool);
                        Multicast.leave(socket, ra, 0, 0);
                    } catch (final Exception e) {

                    }
                }
            }
            return;
        }

    }

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

    @Override
    public final IoSessionRecycler getSessionRecycler() {
        return sessionRecycler;
    }

    @Override
    public final void setSessionRecycler(final IoSessionRecycler sessionRecycler) {
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

    @Override
    protected void init(final SelectorProvider selectorProvider) throws Exception {
        init();
    }

    @Override
    protected AprSession accept(final IoProcessor<AprSession> processor, final Long handle) throws Exception {
        final long ra = Address.get(Socket.APR_REMOTE, handle);
        final InetSocketAddress remoteAddress = new InetSocketAddress(Address.getip(ra), Address.getInfo(ra).port);
        final AprDatagramSession ds = new AprDatagramSession(this, processor, handle, remoteAddress);
        return ds;
    }

}
