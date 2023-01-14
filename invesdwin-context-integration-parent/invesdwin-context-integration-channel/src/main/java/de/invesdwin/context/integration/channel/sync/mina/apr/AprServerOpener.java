package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.mina.core.RuntimeIoException;
import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Error;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.date.FTimeUnit;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

@NotThreadSafe
public final class AprServerOpener {

    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    public static final int APR_TIMEUP_ERROR = -120001;
    public static final int POLLSET_SIZE = 1024;

    private AprServerOpener() {}

    public static void openServer(final TomcatNativeSocketSynchronousChannel channel) {
        try {
            final long acceptorHandle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_TCP,
                    channel.getFinalizer().getPool());
            long pollset = 0;

            try {
                openAcceptor(channel, acceptorHandle);

                pollset = openPollset(channel);
                channel.getFinalizer().setPollset(pollset);

                final int result = Poll.add(pollset, acceptorHandle, Poll.APR_POLLIN);
                if (result != Status.APR_SUCCESS) {
                    throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
                }

                final LongList polledHandles = selectRetry(channel, pollset);
                final long handle = polledHandles.getLong(0);
                accept(handle);
                Socket.optSet(handle, Socket.APR_SO_NONBLOCK, 1);
                Socket.timeoutSet(handle, 0);
                channel.getFinalizer().setFd(handle);
            } finally {
                //unbind
                if (pollset > 0) {
                    Poll.remove(pollset, acceptorHandle);
                    Poll.destroy(pollset);
                }
                TomcatNativeSocketSynchronousChannel.closeHandle(acceptorHandle);
                System.out.println("server connected");
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void openAcceptor(final TomcatNativeSocketSynchronousChannel channel, final long acceptorHandle)
            throws IOException, Exception, Error {
        int result = Socket.optSet(acceptorHandle, Socket.APR_SO_NONBLOCK, 1);
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }
        result = Socket.timeoutSet(acceptorHandle, 0);
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }

        // Configure the server socket,
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_REUSEADDR, 0);
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_RCVBUF,
                Integers.max(Socket.optGet(acceptorHandle, Socket.APR_SO_RCVBUF), ByteBuffers.calculateExpansion(
                        channel.getSocketSize() * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }

        // and bind.
        final long sa;
        final InetSocketAddress la = channel.getSocketAddress();
        if (la != null) {
            if (la.getAddress() == null) {
                sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, la.getPort(), 0,
                        channel.getFinalizer().getPool());
            } else {
                sa = Address.info(la.getAddress().getHostAddress(), Socket.APR_INET, la.getPort(), 0,
                        channel.getFinalizer().getPool());
            }
        } else {
            sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, 0, 0, channel.getFinalizer().getPool());
        }

        result = Socket.bind(acceptorHandle, sa);
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }
        result = Socket.listen(acceptorHandle, 1);
        if (result != Status.APR_SUCCESS) {
            throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
        }
    }

    public static long openPollset(final TomcatNativeSocketSynchronousChannel channel) throws Error {
        long pollset = Poll.create(POLLSET_SIZE, channel.getFinalizer().getPool(), Poll.APR_POLLSET_THREADSAFE,
                Long.MAX_VALUE);

        if (pollset <= 0) {
            pollset = Poll.create(62, channel.getFinalizer().getPool(), Poll.APR_POLLSET_THREADSAFE, Long.MAX_VALUE);
        }

        if (pollset <= 0) {
            if (Status.APR_STATUS_IS_ENOTIMPL(-(int) pollset)) {
                throw new RuntimeIoException("Thread-safe pollset is not supported in this platform.");
            }
        }
        return pollset;
    }

    private static LongList selectRetry(final TomcatNativeSocketSynchronousChannel channel, final long pollset)
            throws Exception, IOException {
        final long[] polledSockets = new long[POLLSET_SIZE << 1];
        final LongList polledHandles = new LongArrayList();

        final long startNanos = System.nanoTime();
        while (polledHandles.isEmpty()) {
            select(channel, pollset, polledSockets, polledHandles);
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
        return polledHandles;
    }

    private static int select(final TomcatNativeSocketSynchronousChannel channel, final long pollset,
            final long[] polledSockets, final LongList polledHandles) throws Exception {
        int rv = Poll.poll(pollset, channel.getMaxConnectRetryDelay().longValue(FTimeUnit.MICROSECONDS), polledSockets,
                false);
        if (rv <= 0) {
            // We have had an error. It can simply be that we have reached
            // the timeout (very unlikely, as we have set it to MAX_INTEGER)
            if (rv != APR_TIMEUP_ERROR) {
                // It's not a timeout being exceeded. Throw the error
                throw TomcatNativeSocketSynchronousChannel.newTomcatException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    Poll.add(pollset, polledSockets[i], Poll.APR_POLLIN);
                }
            } else if (rv < 0) {
                throw TomcatNativeSocketSynchronousChannel.newTomcatException(rv);
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

    private static long accept(final long handle) throws Exception {
        final long s = Socket.accept(handle);
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

}
