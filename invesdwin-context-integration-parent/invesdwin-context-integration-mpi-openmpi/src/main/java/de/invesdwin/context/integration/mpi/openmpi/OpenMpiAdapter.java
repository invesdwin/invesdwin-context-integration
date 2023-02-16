package de.invesdwin.context.integration.mpi.openmpi;

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
public class OpenMpiAdapter implements IMpiAdapter {

    private final Supplier<Intracomm> comm;

    public OpenMpiAdapter() {
        this.comm = () -> MPI.COMM_WORLD;
    }

    public OpenMpiAdapter(final Supplier<Intracomm> comm) {
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
            final int support = MPI.InitThread(args, OpenMpiThreadSupports.toMpi(required));
            return OpenMpiThreadSupports.fromMpi(support);
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
        return new OpenMpiBcastSynchronousWriter(getComm(), root, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newBcastReader(final IIntReference root, final int maxMessageSize) {
        return new OpenMpiBcastSynchronousReader(getComm(), root, maxMessageSize);
    }

    @Override
    public ISynchronousWriter<IByteBufferProvider> newSendWriter(final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        return new OpenMpiSendSynchronousWriter(getComm(), dest, tag, maxMessageSize);
    }

    @Override
    public ISynchronousReader<IByteBufferProvider> newRecvReader(final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        return new OpenMpiRecvSynchronousReader(getComm(), source, tag, maxMessageSize);
    }

    @Override
    public IMpiAdapter split(final int color, final int key) {
        try {
            final Intracomm split = getComm().split(color, key);
            return new OpenMpiAdapter(() -> split);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort(final int errorCode) {
        try {
            getComm().abort(errorCode);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void finalizeMpi() {
        try {
            MPI.Finalize();
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

}
