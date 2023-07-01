package io.netty.channel;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class ChannelHandlerAdapterAccessor {

    private ChannelHandlerAdapterAccessor() {}

    public static void setAdded(final ChannelHandlerAdapter adapter, final boolean added) {
        adapter.added = added;
    }

}
