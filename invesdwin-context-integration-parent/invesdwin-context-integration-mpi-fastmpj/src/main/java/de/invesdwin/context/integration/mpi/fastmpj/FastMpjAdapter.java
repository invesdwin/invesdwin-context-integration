package de.invesdwin.context.integration.mpi.fastmpj;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;

@Immutable
public class FastMpjAdapter implements IMpiAdapter {

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        MPI.Init(args);
        final int support = MPI.queryThread();
        return MpiThreadSupports.fromMpi(support);
    }

    @Override
    public int rank() {
        return MPI.COMM_WORLD.Rank();
    }

    @Override
    public int size() {
        return MPI.COMM_WORLD.Size();
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
        MPI.COMM_WORLD.Barrier();
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root) {
        return new FastMpjBcastSynchronousWriter(root);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new FastMpjBcastSynchronousReader(root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag) {
        return new FastMpjSendSynchronousWriter(dest, tag);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IIntReference source, final IIntReference tag,
            final int maxMessageSize) {
        return new FastMpjRecvSynchronousReader(source, tag, maxMessageSize);
    }

    @Override
    public void abort(final int errorCode) {
        MPI.COMM_WORLD.Abort(errorCode);
    }

    @Override
    public void close() {
        MPI.Finalize();
    }

}
