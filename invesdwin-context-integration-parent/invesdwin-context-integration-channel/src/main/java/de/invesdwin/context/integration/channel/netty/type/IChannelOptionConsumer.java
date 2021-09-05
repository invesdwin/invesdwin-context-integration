package de.invesdwin.context.integration.channel.netty.type;

import io.netty.channel.ChannelOption;

public interface IChannelOptionConsumer {

    <T> void option(ChannelOption<T> option, T value);

}
