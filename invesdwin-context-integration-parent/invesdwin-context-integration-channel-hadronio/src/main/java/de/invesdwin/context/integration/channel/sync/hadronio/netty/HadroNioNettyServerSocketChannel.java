package de.invesdwin.context.integration.channel.sync.hadronio.netty;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.hadronio.HadronioSocketSynchronousChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;

@NotThreadSafe
public class HadroNioNettyServerSocketChannel extends NioServerSocketChannel {

    public HadroNioNettyServerSocketChannel() {
        super(HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

}
