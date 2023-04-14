package de.invesdwin.context.integration.mpi.mvapich2;

import java.util.function.Supplier;

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
import mpi.MPIException;

@Immutable
public class Mvapich2Adapter implements IMpiAdapter {

    /**
     * Status only contains gibberish, so we have to encode our own lengths
     */
    public static final int SIZE_INDEX = 0;
    public static final int SIZE_SIZE = Integer.BYTES;

    public static final int MESSAGE_INDEX = SIZE_INDEX + SIZE_SIZE;

    private final Supplier<Intracomm> comm;

    public Mvapich2Adapter() {
        this.comm = () -> MPI.COMM_WORLD;
    }

    public Mvapich2Adapter(final Supplier<Intracomm> comm) {
        this.comm = comm;
    }

    public Intracomm getComm() {
        return comm.get();
    }

    @Override
    public String[] init(final String[] args) {
        try {
            return MPI.Init(args);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MpiThreadSupport initThread(final String[] args, final MpiThreadSupport required) {
        try {
            final int support = MPI.InitThread(args, Mvapich2ThreadSupports.toMpi(required));
            return Mvapich2ThreadSupports.fromMpi(support);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MpiThreadSupport queryThread() {
        try {
            final int support = MPI.queryThread();
            return Mvapich2ThreadSupports.fromMpi(support);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int rank() {
        try {
            return getComm().getRank();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public int size() {
        try {
            return getComm().getSize();
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
            getComm().barrier();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newBcastWriter(final IIntReference root, final int maxMessageSize) {
        return new Mvapich2BcastSynchronousWriter(getComm(), root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new Mvapich2BcastSynchronousReader(getComm(), root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return new Mvapich2SendSynchronousWriter(getComm(), dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return new Mvapich2RecvSynchronousReader(getComm(), source, tag, maxMessageSize);
    }

    @Override
    public IMpiAdapter split(final int color, final int key) {
        try {
            final Intracomm split = getComm().split(color, key);
            return new Mvapich2Adapter(() -> split);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort(final int errorCode) {
        try {
            getComm().Abort(errorCode);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void free() {
        try {
            MPI.Finalize();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

}
