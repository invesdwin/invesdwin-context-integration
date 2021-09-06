package de.invesdwin.context.integration.channel.sync.jeromq.type;

import javax.annotation.concurrent.Immutable;

import org.zeromq.api.SocketType;

@Immutable
public enum JeromqSocketType implements IJeromqSocketType {
    PAIR(SocketType.PAIR, SocketType.PAIR),
    PUBSUB(SocketType.PUB, SocketType.SUB),
    PUSHPULL(SocketType.PUSH, SocketType.PULL);

    private final SocketType writer;
    private final SocketType reader;

    JeromqSocketType(final SocketType writer, final SocketType reader) {
        this.writer = writer;
        this.reader = reader;
    }

    @Override
    public SocketType getWriterSocketType() {
        return writer;
    }

    @Override
    public SocketType getReaderSocketType() {
        return reader;
    }

}
