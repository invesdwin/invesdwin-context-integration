package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.mina.core.service.IoConnector;
import org.apache.mina.transport.socket.apr.AprLibraryAccessor;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Pool;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.time.date.FTimeUnit;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

/**
 * {@link IoConnector} for APR based socket transport (TCP/IP).
 * 
 * @author <a href="http://mina.apache.org">Apache MINA Project</a>
 */
@NotThreadSafe
public final class AprClientOpener2 {

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

    private final Set<Long> failedHandles = new HashSet<Long>(POLLSET_SIZE);

    private volatile ByteBuffer dummyBuffer;

    public void openClient(final TomcatNativeSocketSynchronousChannel channel) throws Exception {
        init();
        final long handle = newHandle();
        if (!connect(handle, channel.getSocketAddress())) {
            register(handle);
            selectRetry(channel);
        }
        finishConnect(handle);

        channel.getFinalizer().setFd(handle);
    }

    private void init() throws Exception {
        // initialize a memory pool for APR functions
        pool = Pool.create(AprLibraryAccessor.getRootPool());

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

    private boolean connect(final long handle, final SocketAddress remoteAddress) throws Exception {
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

    private long newHandle() throws Exception {
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

    private void register(final long handle) throws Exception {
        final int rv = Poll.add(pollset, handle, Poll.APR_POLLOUT);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
        }
    }

    private void selectRetry(final TomcatNativeSocketSynchronousChannel channel) throws Exception {
        final long startNanos = System.nanoTime();
        final long timeoutMicros = channel.getMaxConnectRetryDelay().longValue(FTimeUnit.MICROSECONDS);
        while (polledHandles.isEmpty()) {
            select(timeoutMicros);
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
            TomcatNativeSocketSynchronousChannel.closeHandle(polledHandles.getLong(i));
        }
    }

    private int select(final long timeoutMicros) throws Exception {
        int rv = Poll.poll(pollset, timeoutMicros, polledSockets, false);
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
                polledHandles.add(socket);
                if ((flag & Poll.APR_POLLOUT) == 0) {
                    failedHandles.add(socket);
                }
            }
            return polledHandles.size();
        }
    }

    private boolean finishConnect(final long handle) throws Exception {
        Poll.remove(pollset, handle);
        if (failedHandles.remove(handle)) {
            final int rv = Socket.recvb(handle, dummyBuffer, 0, 1);
            throwException(rv);
            throw new InternalError("Shouldn't reach here.");
        }
        return true;
    }

    private void close(final long handle) throws Exception {
        finishConnect(handle);
        final int rv = Socket.close(handle);
        if (rv != Status.APR_SUCCESS) {
            throwException(rv);
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

    private void throwException(final int code) throws IOException {
        throw TomcatNativeSocketSynchronousChannel.newTomcatException(code);
    }
}
