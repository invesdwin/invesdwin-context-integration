package de.invesdwin.context.integration.channel.sync.netty;

import io.netty.channel.ChannelOption;

public interface IChannelOptionConsumer {

    <T> void option(ChannelOption<T> option, T value);

}
