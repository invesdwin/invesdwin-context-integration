package de.invesdwin.context.integration.mpi.mvapich2;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.integer.IMutableIntReference;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;
import mpi.Status;

@NotThreadSafe
public class Mvapich2RecvSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final Intracomm comm;
    private final IMutableIntReference source;
    private final IMutableIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;
    private Status status;
    private int sourceBefore;
    private int tagBefore;

    public Mvapich2RecvSynchronousReader(final Intracomm comm, final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        this.comm = comm;
        this.source = source;
        this.tag = tag;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirect(Mvapich2Adapter.MESSAGE_INDEX + maxMessageSize);
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        request = null;
        status = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            return false;
        }
        if (request != null) {
            return hasMessage();
        }
        try {
            request = comm.iRecv(buffer.nioByteBuffer(), buffer.capacity(), MPI.BYTE, source.get(), tag.get());
        } catch (final MPIException e) {
            throw new IOException(e);
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (status != null) {
            return true;
        }
        try {
            status = request.testStatus();
            return status != null;
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int length = buffer.getInt(Mvapich2Adapter.SIZE_INDEX);
        if (ClosedByteBuffer.isClosed(buffer, Mvapich2Adapter.MESSAGE_INDEX, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        sourceBefore = source.getAndSet(status.getSource());
        tagBefore = tag.getAndSet(status.getTag());
        return buffer.slice(Mvapich2Adapter.MESSAGE_INDEX, length);
    }

    @Override
    public void readFinished() throws IOException {
        request = null;
        status = null;
        source.set(sourceBefore);
        tag.set(tagBefore);
    }

}
