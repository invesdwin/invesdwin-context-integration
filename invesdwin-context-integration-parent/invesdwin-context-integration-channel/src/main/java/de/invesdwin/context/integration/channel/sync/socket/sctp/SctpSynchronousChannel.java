package de.invesdwin.context.integration.channel.sync.socket.sctp;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.SynchronousChannels;
import de.invesdwin.context.integration.channel.sync.socket.udp.blocking.BlockingDatagramSynchronousChannel;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.finalizer.AWarningFinalizer;
import de.invesdwin.util.lang.reflection.Reflections;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.time.duration.Duration;

@NotThreadSafe
public class SctpSynchronousChannel implements ISynchronousChannel {

    public static final Class<?> MESSAGEINFO_CLASS;
    public static final MethodHandle MESSAGEINFO_BYTES_METHOD;
    public static final Method MESSAGEINFO_CREATEOUTGOING_METHOD;

    public static final Class<?> SCTPCHANNEL_CLASS;
    public static final Method SCTPCHANNEL_OPEN_METHOD;
    public static final MethodHandle SCTPCHANNEL_CONNECT_METHOD;
    public static final MethodHandle SCTPCHANNEL_CONFIGUREBLOCKING_METHOD;
    public static final MethodHandle SCTPCHANNEL_GETOPTION_METHOD;
    public static final MethodHandle SCTPCHANNEL_SETOPTION_METHOD;
    public static final MethodHandle SCTPCHANNEL_RECEIVE_METHOD;
    public static final MethodHandle SCTPCHANNEL_SEND_METHOD;

    public static final Class<?> SCTPSERVERCHANNEL_CLASS;
    public static final Method SCTPSERVERCHANNEL_OPEN_METHOD;
    public static final MethodHandle SCTPSERVERCHANNEL_BIND_METHOD;
    public static final MethodHandle SCTPSERVERCHANNEL_ACCEPT_METHOD;

    public static final Class<?> SCTPSTANDARDSOCKETOPTIONS_CLASS;
    public static final Object SCTPSTANDARDSOCKETOPTIONS_SORCVBUF;
    public static final Object SCTPSTANDARDSOCKETOPTIONS_SOSNDBUF;
    public static final Object SCTPSTANDARDSOCKETOPTIONS_SCTPNODELAY;

    static {
        try {
            //only OracleJDK contains SCTP as it seems (causes compile errors in maven/jenkins): https://stackoverflow.com/a/26614215
            //static native int receive0(int fd, ResultContainer resultContainer, long address, int length, boolean peek) throws IOException;
            MESSAGEINFO_CLASS = Class.forName("com.sun.nio.sctp.MessageInfo");
            final Method messageInfoBytesMethod = Reflections.findMethod(MESSAGEINFO_CLASS, "bytes");
            MESSAGEINFO_BYTES_METHOD = MethodHandles.lookup().unreflect(messageInfoBytesMethod);
            MESSAGEINFO_CREATEOUTGOING_METHOD = Reflections.findMethod(MESSAGEINFO_CLASS, "createOutgoing",
                    SocketAddress.class, int.class);

            SCTPCHANNEL_CLASS = Class.forName("com.sun.nio.sctp.SctpChannel");
            SCTPCHANNEL_OPEN_METHOD = Reflections.findMethod(SCTPCHANNEL_CLASS, "open");
            final Method sctpChannelReceiveMethod = Reflections.findMethodByName(SCTPCHANNEL_CLASS, "receive");
            SCTPCHANNEL_RECEIVE_METHOD = MethodHandles.lookup().unreflect(sctpChannelReceiveMethod);
            final Method sctpChannelSendMethod = Reflections.findMethodByName(SCTPCHANNEL_CLASS, "send");
            SCTPCHANNEL_SEND_METHOD = MethodHandles.lookup().unreflect(sctpChannelSendMethod);
            final Method sctpChannelConnectMethod = Reflections.findMethod(SCTPCHANNEL_CLASS, "connect",
                    SocketAddress.class);
            SCTPCHANNEL_CONNECT_METHOD = MethodHandles.lookup().unreflect(sctpChannelConnectMethod);
            final Method sctpChannelConfigureBlockingMethod = Reflections.findMethod(SCTPCHANNEL_CLASS,
                    "configureBlocking", boolean.class);
            SCTPCHANNEL_CONFIGUREBLOCKING_METHOD = MethodHandles.lookup().unreflect(sctpChannelConfigureBlockingMethod);
            final Method sctpChannelGetOptionMethod = Reflections.findMethodByName(SCTPCHANNEL_CLASS, "getOption");
            SCTPCHANNEL_GETOPTION_METHOD = MethodHandles.lookup().unreflect(sctpChannelGetOptionMethod);
            final Method sctpChannelSetOptionMethod = Reflections.findMethodByName(SCTPCHANNEL_CLASS, "setOption");
            SCTPCHANNEL_SETOPTION_METHOD = MethodHandles.lookup().unreflect(sctpChannelSetOptionMethod);

            SCTPSERVERCHANNEL_CLASS = Class.forName("com.sun.nio.sctp.SctpServerChannel");
            SCTPSERVERCHANNEL_OPEN_METHOD = Reflections.findMethod(SCTPSERVERCHANNEL_CLASS, "open");
            final Method sctpServerChannelBindMethod = Reflections.findMethod(SCTPSERVERCHANNEL_CLASS, "bind",
                    SocketAddress.class);
            SCTPSERVERCHANNEL_BIND_METHOD = MethodHandles.lookup().unreflect(sctpServerChannelBindMethod);
            final Method sctpServerChannelAcceptMethod = Reflections.findMethod(SCTPSERVERCHANNEL_CLASS, "accept");
            SCTPSERVERCHANNEL_ACCEPT_METHOD = MethodHandles.lookup().unreflect(sctpServerChannelAcceptMethod);

            SCTPSTANDARDSOCKETOPTIONS_CLASS = Class.forName("com.sun.nio.sctp.SctpStandardSocketOptions");
            final Field sctpStandardSocketOptionsSoRcvbufField = Reflections.findField(SCTPSTANDARDSOCKETOPTIONS_CLASS,
                    "SO_RCVBUF");
            SCTPSTANDARDSOCKETOPTIONS_SORCVBUF = sctpStandardSocketOptionsSoRcvbufField.get(null);
            final Field sctpStandardSocketOptionsSoSndbufField = Reflections.findField(SCTPSTANDARDSOCKETOPTIONS_CLASS,
                    "SO_SNDBUF");
            SCTPSTANDARDSOCKETOPTIONS_SOSNDBUF = sctpStandardSocketOptionsSoSndbufField.get(null);
            final Field sctpStandardSocketOptionsSctpNodelayField = Reflections
                    .findField(SCTPSTANDARDSOCKETOPTIONS_CLASS, "SCTP_NODELAY");
            SCTPSTANDARDSOCKETOPTIONS_SCTPNODELAY = sctpStandardSocketOptionsSctpNodelayField.get(null);

        } catch (final ClassNotFoundException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    protected final int estimatedMaxMessageSize;
    protected final int socketSize;
    protected volatile boolean socketChannelOpening;
    protected final SocketAddress socketAddress;
    protected final boolean server;
    private final SocketSynchronousChannelFinalizer finalizer;

    private volatile boolean readerRegistered;
    private volatile boolean writerRegistered;
    @GuardedBy("this for modification")
    private final AtomicInteger activeCount = new AtomicInteger();

    public SctpSynchronousChannel(final SocketAddress socketAddress, final boolean server,
            final int estimatedMaxMessageSize) {
        this.socketAddress = socketAddress;
        this.server = server;
        this.estimatedMaxMessageSize = estimatedMaxMessageSize;
        this.socketSize = estimatedMaxMessageSize + MESSAGE_INDEX;
        this.finalizer = new SocketSynchronousChannelFinalizer();
        finalizer.register(this);
    }

    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isServer() {
        return server;
    }

    public int getSocketSize() {
        return socketSize;
    }

    public Object getSocketChannel() {
        return finalizer.socketChannel;
    }

    public Object getServerSocketChannel() {
        return finalizer.serverSocketChannel;
    }

    public boolean isReaderRegistered() {
        return readerRegistered;
    }

    public void setReaderRegistered() {
        if (readerRegistered) {
            throw new IllegalStateException("reader already registered");
        }
        this.readerRegistered = true;
    }

    public boolean isWriterRegistered() {
        return writerRegistered;
    }

    public void setWriterRegistered() {
        if (writerRegistered) {
            throw new IllegalStateException("writer already registered");
        }
        this.writerRegistered = true;
    }

    @Override
    public void open() throws IOException {
        if (!shouldOpen()) {
            awaitSocketChannel();
            return;
        }
        socketChannelOpening = true;
        try {
            if (server) {
                finalizer.serverSocketChannel = newServerSocketChannel();
                SCTPSERVERCHANNEL_BIND_METHOD.invoke(finalizer.serverSocketChannel, socketAddress);
                finalizer.socketChannel = SCTPSERVERCHANNEL_ACCEPT_METHOD.invoke(finalizer.serverSocketChannel);
            } else {
                final Duration connectTimeout = getConnectTimeout();
                final long startNanos = System.nanoTime();
                while (true) {
                    finalizer.socketChannel = newSocketChannel();
                    try {
                        SCTPCHANNEL_CONNECT_METHOD.invoke(finalizer.socketChannel, socketAddress);
                        break;
                    } catch (final IOException e) {
                        Closeables.closeQuietly(finalizer.socketChannel);
                        finalizer.socketChannel = null;
                        if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                            try {
                                getMaxConnectRetryDelay().sleepRandom();
                            } catch (final InterruptedException e1) {
                                throw new IOException(e1);
                            }
                        } else {
                            throw e;
                        }
                    }
                }
            }
            //non-blocking sockets are a bit faster than blocking ones
            SCTPCHANNEL_CONFIGUREBLOCKING_METHOD.invoke(finalizer.socketChannel, false);
            //might be unix domain socket
            final Integer soRcvBuf = (Integer) SCTPCHANNEL_GETOPTION_METHOD.invoke(finalizer.socketChannel,
                    SCTPSTANDARDSOCKETOPTIONS_SORCVBUF);
            final Integer newSoRcvBuf = Integers.max(soRcvBuf, ByteBuffers.calculateExpansion(
                    socketSize * BlockingDatagramSynchronousChannel.RECEIVE_BUFFER_SIZE_MULTIPLIER));
            SCTPCHANNEL_SETOPTION_METHOD.invoke(finalizer.socketChannel, SCTPSTANDARDSOCKETOPTIONS_SORCVBUF,
                    newSoRcvBuf);
            SCTPCHANNEL_SETOPTION_METHOD.invoke(finalizer.socketChannel, SCTPSTANDARDSOCKETOPTIONS_SOSNDBUF,
                    socketSize);
            SCTPCHANNEL_SETOPTION_METHOD.invoke(finalizer.socketChannel, SCTPSTANDARDSOCKETOPTIONS_SCTPNODELAY, true);
        } catch (final IOException e) {
            throw e;
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        } finally {
            socketChannelOpening = false;
        }
    }

    private void awaitSocketChannel() throws IOException {
        try {
            //wait for channel
            final Duration connectTimeout = getConnectTimeout();
            final long startNanos = System.nanoTime();
            while ((finalizer.socketChannel == null || socketChannelOpening) && activeCount.get() > 0) {
                if (connectTimeout.isGreaterThanNanos(System.nanoTime() - startNanos)) {
                    getWaitInterval().sleep();
                } else {
                    throw new ConnectException("Connection timeout");
                }
            }
        } catch (final Throwable t) {
            close();
            throw new IOException(t);
        }
    }

    private synchronized boolean shouldOpen() {
        return activeCount.incrementAndGet() == 1;
    }

    protected Object newSocketChannel() throws IOException {
        try {
            return SCTPCHANNEL_OPEN_METHOD.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object newServerSocketChannel() throws IOException {
        try {
            return SCTPSERVERCHANNEL_OPEN_METHOD.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    protected Duration getMaxConnectRetryDelay() {
        return SynchronousChannels.DEFAULT_MAX_RECONNECT_DELAY;
    }

    protected Duration getConnectTimeout() {
        return SynchronousChannels.DEFAULT_CONNECT_TIMEOUT;
    }

    protected Duration getWaitInterval() {
        return SynchronousChannels.DEFAULT_WAIT_INTERVAL;
    }

    @Override
    public void close() {
        if (!shouldClose()) {
            return;
        }
        finalizer.close();
    }

    private synchronized boolean shouldClose() {
        final int activeCountBefore = activeCount.get();
        if (activeCountBefore > 0) {
            activeCount.decrementAndGet();
        }
        return activeCountBefore == 1;
    }

    private static final class SocketSynchronousChannelFinalizer extends AWarningFinalizer {

        private volatile Object socketChannel;
        private volatile Object serverSocketChannel;

        @Override
        protected void clean() {
            final Object socketChannelCopy = socketChannel;
            if (socketChannelCopy != null) {
                socketChannel = null;
                Closeables.closeQuietly(socketChannelCopy);
            }
            final Object serverSocketChannelCopy = serverSocketChannel;
            if (serverSocketChannelCopy != null) {
                serverSocketChannel = null;
                Closeables.closeQuietly(serverSocketChannelCopy);
            }
        }

        @Override
        protected boolean isCleaned() {
            return socketChannel == null;
        }

        @Override
        public boolean isThreadLocal() {
            return false;
        }

    }

}
