package de.invesdwin.context.integration.channel.sync.chronicle.network.type;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.util.lang.reflection.Reflections;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleServerSocketFactory;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannelFactory;
import net.openhft.chronicle.network.tcp.FastJ8SocketChannel;
import net.openhft.chronicle.network.tcp.UnsafeFastJ8SocketChannel;
import net.openhft.chronicle.network.tcp.VanillaChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.VanillaSocketChannel;

@Immutable
public enum ChronicleSocketChannelType implements IChronicleSocketChannelType {
    DEFAULT {
        @Override
        public ChronicleSocketChannel newSocketChannel(final SocketChannel socketChannel) throws IOException {
            return ChronicleSocketChannelFactory.wrap(socketChannel);
        }
    },
    NATIVE {
        @Override
        public ChronicleSocketChannel newSocketChannel(final SocketChannel socketChannel) throws IOException {
            return ChronicleSocketChannelFactory.wrap(true, socketChannel);
        }
    },
    VANILLA {
        @Override
        public ChronicleSocketChannel newSocketChannel(final SocketChannel socketChannel) throws IOException {
            return new VanillaSocketChannel(socketChannel);
        }
    },
    FAST {
        @Override
        public ChronicleSocketChannel newSocketChannel(final SocketChannel socketChannel) throws IOException {
            return new FastJ8SocketChannel(socketChannel);
        }
    },
    UNSAFE_FAST {
        @Override
        public ChronicleSocketChannel newSocketChannel(final SocketChannel socketChannel) throws IOException {
            return new UnsafeFastJ8SocketChannel(socketChannel);
        }
    };

    public static final MethodHandle VANILLA_SSC_FIELD_GETTER;

    static {
        final Field field = Reflections.findField(VanillaChronicleServerSocketChannel.class, "ssc");
        field.setAccessible(true);
        try {
            VANILLA_SSC_FIELD_GETTER = MethodHandles.lookup().unreflectGetter(field);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ChronicleServerSocketChannel newServerSocketChannel() {
        return ChronicleServerSocketFactory.open();
    }

    @Override
    public ChronicleSocketChannel acceptSocketChannel(final ChronicleServerSocketChannel serverSocketChannel)
            throws IOException {
        if (serverSocketChannel instanceof VanillaChronicleServerSocketChannel) {
            final VanillaChronicleServerSocketChannel cServerSocketChannel = (VanillaChronicleServerSocketChannel) serverSocketChannel;
            final ServerSocketChannel ssc = unwrapServerSocketChannel(cServerSocketChannel);
            ssc.configureBlocking(true); //vanilla impl does the same
            final SocketChannel accept = ssc.accept();
            return newSocketChannel(accept);
        } else {
            //native
            return serverSocketChannel.accept();
        }
    }

    public static ServerSocketChannel unwrapServerSocketChannel(
            final VanillaChronicleServerSocketChannel serverSocketChannel) {
        try {
            return (ServerSocketChannel) VANILLA_SSC_FIELD_GETTER.invoke(serverSocketChannel);
        } catch (final Throwable e) {
            throw new RuntimeException();
        }
    }

}
