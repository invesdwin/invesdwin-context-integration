package de.invesdwin.context.integration.channel.sync.mina.apr;

import java.io.IOException;
import java.net.InetSocketAddress;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.tomcat.jni.Address;
import org.apache.tomcat.jni.Error;
import org.apache.tomcat.jni.Socket;
import org.apache.tomcat.jni.Status;

import de.invesdwin.context.integration.channel.sync.mina.MinaSocketSynchronousChannel;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;

@NotThreadSafe
public final class MinaNativeDatagramServerOpener {

    /**
     * This constant is deduced from the APR code. It is used when the timeout has expired while doing a poll()
     * operation.
     */
    public static final int APR_TIMEUP_ERROR = -120001;
    public static final int POLLSET_SIZE = 1024;

    private MinaNativeDatagramServerOpener() {}

    public static void openServer(final MinaNativeDatagramSynchronousChannel channel) {
        try {
            final long acceptorHandle = Socket.create(Socket.APR_INET, Socket.SOCK_DGRAM, Socket.APR_PROTO_UDP,
                    channel.getFinalizer().getPool());
            openAcceptor(channel, acceptorHandle);
            channel.getFinalizer().setFd(acceptorHandle);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void openAcceptor(final MinaNativeDatagramSynchronousChannel channel, final long acceptorHandle)
            throws IOException, Exception, Error {
        int result = Socket.optSet(acceptorHandle, Socket.APR_SO_NONBLOCK, 1);
        if (result != Status.APR_SUCCESS) {
            throw MinaSocketSynchronousChannel.newTomcatException(result);
        }
        result = Socket.timeoutSet(acceptorHandle, 0);
        if (result != Status.APR_SUCCESS) {
            throw MinaSocketSynchronousChannel.newTomcatException(result);
        }

        // Configure the server socket,
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_REUSEADDR, 0);
        if (result != Status.APR_SUCCESS) {
            throw MinaSocketSynchronousChannel.newTomcatException(result);
        }
        result = Socket.optSet(acceptorHandle, Socket.APR_SO_RCVBUF,
                Integers.max(Socket.optGet(acceptorHandle, Socket.APR_SO_RCVBUF), ByteBuffers.calculateExpansion(
                        channel.getSocketSize() * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER)));
        if (result != Status.APR_SUCCESS) {
            throw MinaSocketSynchronousChannel.newTomcatException(result);
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
            throw MinaSocketSynchronousChannel.newTomcatException(result);
        }
    }

}
