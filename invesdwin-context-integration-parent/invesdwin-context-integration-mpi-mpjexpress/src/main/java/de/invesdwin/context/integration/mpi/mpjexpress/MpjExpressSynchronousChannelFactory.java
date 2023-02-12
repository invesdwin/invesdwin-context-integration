package de.invesdwin.context.integration.mpi.mpjexpress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiSynchronousChannelFactory;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;

@Immutable
public class MpjExpressSynchronousChannelFactory implements IMpiSynchronousChannelFactory {

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        MPI.Init(args);
        final int support = MPI.queryThread();
        return MpiThreadSupports.fromMpi(support);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcast() {
        return null;
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSend() {
        return null;
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newReceive() {
        return null;
    }

    @Override
    public void close() {
        MPI.Finalize();
    }

}
