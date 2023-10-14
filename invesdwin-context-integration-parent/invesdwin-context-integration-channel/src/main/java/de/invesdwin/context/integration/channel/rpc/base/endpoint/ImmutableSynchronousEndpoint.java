package de.invesdwin.context.integration.channel.rpc.base.endpoint;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;

@Immutable
public final class ImmutableSynchronousEndpoint<R, W> implements ISynchronousEndpoint<R, W> {

    private final ISynchronousReader<R> reader;
    private final ISynchronousWriter<W> writer;

    private ImmutableSynchronousEndpoint(final ISynchronousReader<R> reader, final ISynchronousWriter<W> writer) {
        this.reader = reader;
        this.writer = writer;
    }

    @Override
    public ISynchronousReader<R> getReader() {
        return reader;
    }

    @Override
    public ISynchronousWriter<W> getWriter() {
        return writer;
    }

    public static <R, W> ImmutableSynchronousEndpoint<R, W> of(final ISynchronousReader<R> reader,
            final ISynchronousWriter<W> writer) {
        return new ImmutableSynchronousEndpoint<>(reader, writer);
    }

}
