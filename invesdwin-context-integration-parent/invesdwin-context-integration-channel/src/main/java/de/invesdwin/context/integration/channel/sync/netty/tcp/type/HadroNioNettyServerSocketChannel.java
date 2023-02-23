package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ucx.hadronio.HadronioSocketSynchronousChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

@NotThreadSafe
public class HadroNioNettyServerSocketChannel extends NioServerSocketChannel {

    public HadroNioNettyServerSocketChannel() {
        super(HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

}
