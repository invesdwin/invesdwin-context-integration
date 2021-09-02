package de.invesdwin.context.integration.channel.chronicle.network.type;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import net.openhft.chronicle.network.tcp.ChronicleServerSocketChannel;
import net.openhft.chronicle.network.tcp.ChronicleSocketChannel;

public interface IChronicleSocketChannelType {

    ChronicleSocketChannel newSocketChannel(SocketChannel socketChannel) throws IOException;

    ChronicleServerSocketChannel newServerSocketChannel();

    ChronicleSocketChannel acceptSocketChannel(ChronicleServerSocketChannel serverSocketChannel) throws IOException;

}
