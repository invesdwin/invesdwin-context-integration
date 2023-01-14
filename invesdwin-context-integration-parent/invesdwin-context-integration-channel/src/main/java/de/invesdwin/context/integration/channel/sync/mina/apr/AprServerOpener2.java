package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.transport.socket.apr.AprLibraryAccessor;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * {@link IoAcceptor} for APR based socket transport (TCP/IP).
 *
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@NotThreadSafe
public final class AprServerOpener2 {
    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    private static final int APR_TIMEUP_ERROR = -120001;

    private static final int POLLSET_SIZE = 1024;

    private volatile long pool;

    private volatile long pollset; // socket poller

    private final long[] polledSockets = new long[POLLSET_SIZE << 1];

    private final LongList polledHandles = new LongArrayList();

    public void openServer(final TomcatNativeSocketSynchronousChannel channel) throws Exception {
        init();
        final long acceptorHandle = open(channel.getSocketAddress(), channel.getSocketSize());
        final long handle = selectRetry(channel);
        accept(handle);
        channel.getFinalizer().setFd(handle);
    }

    private void init() throws Exception {
        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibraryAccessor.getRootPool());

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

    private long open(final SocketAddress localAddress, final int socketSize) throws Exception {
        final InetSocketAddress la = (InetSocketAddress) localAddress;
        final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_TCP, pool);

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
            result = Socket.optSet(handle, Socket.APR_SO_REUSEADDR, 0);
            if (result != Status.APR_SUCCESS) {
                throwException(result);
            }
            result = Socket.optSet(handle, Socket.APR_SO_RCVBUF,
                    Integers.max(Socket.optGet(handle, Socket.APR_SO_RCVBUF), ByteBuffers.calculateExpansion(
                            socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
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
            result = Socket.listen(handle, 1);
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

    private long accept(final long acceptorHandle) throws Exception {
        final long s = Socket.accept(acceptorHandle);
        boolean success = false;
        try {
            success = true;
            return s;
        } finally {
            if (!success) {
                Socket.close(s);
            }
        }
    }

    private void destroy() throws Exception {
        if (pollset > 0) {
            Poll.destroy(pollset);
        }
        if (pool > 0) {
            Pool.destroy(pool);
        }
    }

    private long selectRetry(final TomcatNativeSocketSynchronousChannel channel) throws Exception {
        final long startNanos = System.nanoTime();
        while (polledHandles.isEmpty()) {
            select();
            if (polledHandles.isEmpty()) {
                if (Threads.isInterrupted()
                        || channel.getConnectTimeout().isLessThanNanos(System.nanoTime() - startNanos)) {
                    throw new RuntimeException("timeout exceeded");
                }
                try {
                    channel.getMaxConnectRetryDelay().sleepRandom();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        //allow only one connection
        for (int i = 1; i < polledHandles.size(); i++) {
            TomcatNativeSocketSynchronousChannel.closeHandle(polledHandles.get(i));
        }
        return polledHandles.getLong(0);
    }

    private int select() throws Exception {
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

                if ((flag & Poll.APR_POLLIN) != 0) {
                    polledHandles.add(socket);
                }
            }
            return polledHandles.size();
        }
    }

    private void close(final long handle) throws Exception {
        Poll.remove(pollset, handle);
        final int result = Socket.close(handle);
        if (result != Status.APR_SUCCESS) {
            throwException(result);
        }
    }

    private void throwException(final int code) throws IOException {
        throw TomcatNativeSocketSynchronousChannel.newTomcatException(code);
    }

}
