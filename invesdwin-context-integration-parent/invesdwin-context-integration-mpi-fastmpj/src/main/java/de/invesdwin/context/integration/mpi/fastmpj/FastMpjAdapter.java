package de.invesdwin.context.integration.mpi.fastmpj;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.concurrent.reference.integer.IMutableIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;

@Immutable
public class FastMpjAdapter implements IMpiAdapter {

    private final Intracomm comm;

    public FastMpjAdapter() {
        this.comm = MPI.COMM_WORLD;
    }

    public FastMpjAdapter(final Intracomm comm) {
        this.comm = comm;
    }

    public Intracomm getComm() {
        return comm;
    }

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        MPI.Init(args);
        final int support = MPI.queryThread();
        return FastMpjThreadSupports.fromMpi(support);
    }

    @Override
    public int rank() {
        return comm.Rank();
    }

    @Override
    public int size() {
        return comm.Size();
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
        comm.Barrier();
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root, final int maxMessageSize) {
        return new FastMpjBcastSynchronousWriter(comm, root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new FastMpjBcastSynchronousReader(comm, root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return new FastMpjSendSynchronousWriter(comm, dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return new FastMpjRecvSynchronousReader(comm, source, tag, maxMessageSize);
    }

    @Override
    public IMpiAdapter split(final int color, final int key) {
        return new FastMpjAdapter(comm.Split(color, key));
    }

    @Override
    public void abort(final int errorCode) {
        comm.Abort(errorCode);
    }

    @Override
    public void close() {
        MPI.Finalize();
    }

}
