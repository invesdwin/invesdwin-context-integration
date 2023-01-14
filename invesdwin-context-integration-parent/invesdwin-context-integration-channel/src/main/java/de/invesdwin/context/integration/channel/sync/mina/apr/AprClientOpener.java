package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Poll;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.util.concurrent.Threads;
import de.invesdwin.util.math.Bytes;
import de.invesdwin.util.time.date.FTimeUnit;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

@NotThreadSafe
public final class AprClientOpener {

    private AprClientOpener() {}

    public static void openClient(final TomcatNativeSocketSynchronousChannel channel) {
        try {
            final long handle = Socket.create(Socket.APR_INET, Socket.SOCK_STREAM, Socket.APR_PROTO_TCP,
                    channel.getFinalizer().getPool());
            boolean success = false;

            long pollset = 0;
            try {
                pollset = AprServerOpener.openPollset(channel);
                channel.getFinalizer().setPollset(pollset);

                int result = Socket.optSet(handle, Socket.APR_SO_NONBLOCK, 1);
                if (result != Status.APR_SUCCESS) {
                    throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
                }
                result = Socket.timeoutSet(handle, 0);
                if (result != Status.APR_SUCCESS) {
                    throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
                }

                if (!connect(channel, handle)) {
                    result = Poll.add(pollset, handle, Poll.APR_POLLOUT);
                    if (result != Status.APR_SUCCESS) {
                        throw TomcatNativeSocketSynchronousChannel.newTomcatException(result);
                    }
                    selectRetry(channel, pollset);
                }

                //validate connection
                final int count = Socket.recv(handle, Bytes.EMPTY_ARRAY, 0, 1);
                if (count < 0 && !Status.APR_STATUS_IS_EAGAIN(-count) && !Status.APR_STATUS_IS_EOF(-count)) { // EOF
                    throw new RuntimeException(TomcatNativeSocketSynchronousChannel.newTomcatException(count));
                }
                success = true;
                channel.getFinalizer().setFd(handle);
            } finally {
                if (!success) {
                    try {
                        TomcatNativeSocketSynchronousChannel.closeHandle(handle);
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                if (pollset > 0) {
                    Poll.remove(pollset, handle);
                    Poll.destroy(pollset);
                }
            }
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        System.out.println("client connected");
    }

    private static boolean connect(final TomcatNativeSocketSynchronousChannel channel, final long handle)
            throws Exception {
        final InetSocketAddress ra = channel.getSocketAddress();
        final long sa;
        if (ra != null) {
            if (ra.getAddress() == null) {
                sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, ra.getPort(), 0,
                        channel.getFinalizer().getPool());
            } else {
                sa = Address.info(ra.getAddress().getHostAddress(), Socket.APR_INET, ra.getPort(), 0,
                        channel.getFinalizer().getPool());
            }
        } else {
            sa = Address.info(Address.APR_ANYADDR, Socket.APR_INET, 0, 0, channel.getFinalizer().getPool());
        }

        final int rv = Socket.connect(handle, sa);
        if (rv == Status.APR_SUCCESS) {
            return true;
        }

        if (Status.APR_STATUS_IS_EINPROGRESS(rv)) {
            return false;
        }

        throw TomcatNativeSocketSynchronousChannel.newTomcatException(rv);
    }

    private static LongList selectRetry(final TomcatNativeSocketSynchronousChannel channel, final long pollset)
            throws Exception, IOException {
        final long[] polledSockets = new long[AprServerOpener.POLLSET_SIZE << 1];
        final LongList polledHandles = new LongArrayList();

        final long startNanos = System.nanoTime();
        while (polledHandles.isEmpty()) {
            final int ret = select(channel, pollset, polledSockets, polledHandles);
            if (ret < 0) {
                return null;
            }
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
            if (rv != AprServerOpener.APR_TIMEUP_ERROR) {
                throw TomcatNativeSocketSynchronousChannel.newTomcatException(rv);
            }

            rv = Poll.maintain(pollset, polledSockets, true);
            if (rv > 0) {
                for (int i = 0; i < rv; i++) {
                    Poll.add(pollset, polledSockets[i], Poll.APR_POLLOUT);
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
                polledHandles.add(socket);
                if ((flag & Poll.APR_POLLOUT) == 0) {
                    return -1;
                }
            }
            return polledHandles.size();
        }
    }

}
