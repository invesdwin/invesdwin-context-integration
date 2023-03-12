package de.invesdwin.context.integration.channel.sync.hadronio.netty;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.hadronio.HadronioSocketSynchronousChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

@NotThreadSafe
public class HadroNioNettySocketChannel extends NioSocketChannel {

    public HadroNioNettySocketChannel() {
        super(HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

}
