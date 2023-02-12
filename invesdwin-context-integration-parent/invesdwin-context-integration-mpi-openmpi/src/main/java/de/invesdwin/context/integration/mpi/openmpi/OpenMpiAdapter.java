package de.invesdwin.context.integration.mpi.openmpi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.concurrent.reference.integer.IMutableIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.MPIException;

@Immutable
public class OpenMpiAdapter implements IMpiAdapter {

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
    public int rank() {
        try {
            return MPI.COMM_WORLD.getRank();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        try {
            return MPI.COMM_WORLD.getSize();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int anySource() {
        return MPI.ANY_SOURCE;
    }

    @Override
    public int anyTag() {
        return MPI.ANY_TAG;
    }

    @Override
    public void barrier() {
        try {
            MPI.COMM_WORLD.barrier();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root, final int maxMessageSize) {
        return new OpenMpiBcastSynchronousWriter(root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new OpenMpiBcastSynchronousReader(root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return new OpenMpiSendSynchronousWriter(dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return new OpenMpiRecvSynchronousReader(source, tag, maxMessageSize);
    }

    @Override
    public void abort(final int errorCode) {
        try {
            MPI.COMM_WORLD.abort(errorCode);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
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
