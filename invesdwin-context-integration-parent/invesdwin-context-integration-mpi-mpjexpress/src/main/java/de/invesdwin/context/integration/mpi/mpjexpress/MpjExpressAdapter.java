package de.invesdwin.context.integration.mpi.mpjexpress;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.concurrent.reference.integer.IMutableIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;

@Immutable
public class MpjExpressAdapter implements IMpiAdapter {

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
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root, final int maxMessageSize) {
        return new MpjExpressBcastSynchronousWriter(root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new MpjExpressBcastSynchronousReader(root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return new MpjExpressSendSynchronousWriter(dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return new MpjExpressRecvSynchronousReader(source, tag, maxMessageSize);
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
