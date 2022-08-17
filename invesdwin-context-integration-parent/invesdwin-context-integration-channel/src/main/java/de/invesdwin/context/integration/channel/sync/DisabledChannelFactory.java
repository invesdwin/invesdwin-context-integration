package de.invesdwin.context.integration.channel.sync;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class DisabledChannelFactory<R, W> implements ISynchronousChannelFactory<R, W> {

    @SuppressWarnings("rawtypes")
    private static final DisabledChannelFactory INSTANCE = new DisabledChannelFactory<>();

    @Override
    public ISynchronousReader<R> newReader(final ISynchronousReader<R> reader) {
        return reader;
    }

    @Override
    public ISynchronousWriter<W> newWriter(final ISynchronousWriter<W> writer) {
        return writer;
    }

    @SuppressWarnings("unchecked")
    public static <_R, _W> DisabledChannelFactory<_R, _W> getInstance() {
        return INSTANCE;
    }

}
