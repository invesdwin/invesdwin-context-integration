package de.invesdwin.context.integration.mpi;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IMpiSynchronousChannelFactory {

    ISynchronousReader<IByteBufferProvider> newISend();

    ISynchronousWriter<IByteBufferProvider> newIReceive();

}
