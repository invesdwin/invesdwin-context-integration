package de.invesdwin.context.integration.channel.sync;

import javax.annotation.concurrent.Immutable;

@Immutable
public class IgnoreOpenCloseChannelFactory<R, W> implements ISynchronousChannelFactory<R, W> {

    @SuppressWarnings("rawtypes")
    private static final IgnoreOpenCloseChannelFactory INSTANCE = new IgnoreOpenCloseChannelFactory<>();

    @Override
    public ISynchronousReader<R> newReader(final ISynchronousReader<R> reader) {
        return IgnoreOpenCloseSynchronousReader.valueOf(reader);
    }

    @Override
    public ISynchronousWriter<W> newWriter(final ISynchronousWriter<W> writer) {
        return IgnoreOpenCloseSynchronousWriter.valueOf(writer);
    }

    @SuppressWarnings("unchecked")
    public static <_R, _W> IgnoreOpenCloseChannelFactory<_R, _W> getInstance() {
        return INSTANCE;
    }

}
