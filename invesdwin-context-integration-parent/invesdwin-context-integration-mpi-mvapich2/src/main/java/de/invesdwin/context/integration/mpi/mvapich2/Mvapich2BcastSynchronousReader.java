package de.invesdwin.context.integration.mpi.mvapich2;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.error.FastEOFException;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;
import mpi.Status;

@NotThreadSafe
public class Mvapich2BcastSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference root;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Status status;

    public Mvapich2BcastSynchronousReader(final Intracomm comm, final IIntReference root, final int maxMessageSize) {
        this.comm = comm;
        this.root = root;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirect(maxMessageSize);
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("closed");
        }
        return true;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        status = Mvapich2Broadcast.mstBroadcast(comm, buffer.nioByteBuffer(), buffer.capacity(), MPI.BYTE, root.get());
        final int length = status.getCount(MPI.BYTE);
        if (ClosedByteBuffer.isClosed(buffer, 0, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.sliceTo(length);
    }

    @Override
    public void readFinished() throws IOException {
        status = null;
    }

}
