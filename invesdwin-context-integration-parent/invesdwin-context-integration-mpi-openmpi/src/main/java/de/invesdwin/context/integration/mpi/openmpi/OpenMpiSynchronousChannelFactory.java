package de.invesdwin.context.integration.mpi.openmpi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiSynchronousChannelFactory;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.MPIException;

@Immutable
public class OpenMpiSynchronousChannelFactory implements IMpiSynchronousChannelFactory {

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        try {
            final int support = MPI.InitThread(args, MpiThreadSupports.toMpi(required));
            return MpiThreadSupports.fromMpi(support);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
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
        try {
            MPI.Finalize();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

}
