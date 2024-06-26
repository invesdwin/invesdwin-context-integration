package de.invesdwin.context.integration.mpi.mpjexpress;

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
public class MpjExpressBcastSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference root;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Status status;

    public MpjExpressBcastSynchronousReader(final Intracomm comm, final IIntReference root, final int maxMessageSize) {
        this.comm = comm;
        this.root = root;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocate(maxMessageSize);
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
        status = MpjExpressBroadcast.mstBroadcast(comm, buffer.byteArray(), 0, buffer.capacity(), MPI.BYTE, root.get());
        final int length = status.Get_count(MPI.BYTE);
        if (ClosedByteBuffer.isClosed(buffer, 0, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.sliceTo(length);
    }

    @Override
    public void readFinished() throws IOException {
        status.free();
        status = null;
    }

}
