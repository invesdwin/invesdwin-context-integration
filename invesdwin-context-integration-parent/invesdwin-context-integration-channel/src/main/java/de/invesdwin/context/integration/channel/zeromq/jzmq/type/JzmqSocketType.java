package de.invesdwin.context.integration.channel.zeromq.jzmq.type;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.zeromq.ZeromqSocketTypes;

@Immutable
public enum JzmqSocketType implements IJzmqSocketType {
    PAIR(ZeromqSocketTypes.PAIR, ZeromqSocketTypes.PAIR),
    PUBSUB(ZeromqSocketTypes.PUB, ZeromqSocketTypes.SUB),
    PUSHPULL(ZeromqSocketTypes.PUSH, ZeromqSocketTypes.PULL);

    private final int writer;
    private final int reader;

    JzmqSocketType(final int writer, final int reader) {
        this.writer = writer;
        this.reader = reader;
    }

    @Override
    public int getWriterSocketType() {
        return writer;
    }

    @Override
    public int getReaderSocketType() {
        return reader;
    }

}
