package de.invesdwin.context.integration.channel.sync;

public interface ISynchronousChannelFactory<R, W> {

    ISynchronousReader<R> newReader(ISynchronousReader<R> reader);

    ISynchronousWriter<W> newWriter(ISynchronousWriter<W> writer);

}
