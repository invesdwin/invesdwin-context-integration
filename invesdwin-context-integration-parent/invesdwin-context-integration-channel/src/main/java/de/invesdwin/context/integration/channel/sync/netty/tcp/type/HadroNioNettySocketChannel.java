package de.invesdwin.context.integration.channel.sync.netty.tcp.type;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ucx.hadronio.HadronioSocketSynchronousChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@NotThreadSafe
public class HadroNioNettySocketChannel extends NioSocketChannel {

    public HadroNioNettySocketChannel() {
        super(HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

}
