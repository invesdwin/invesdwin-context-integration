package de.invesdwin.context.integration.mpi.mpjexpress;

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
import mpi.Request;
import mpi.Status;

@NotThreadSafe
public class MpjExpressRecvSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final Intracomm comm;
    private final IMutableIntReference source;
    private final IMutableIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;
    private Status status;
    private int sourceBefore;
    private int tagBefore;

    public MpjExpressRecvSynchronousReader(final Intracomm comm, final IMutableIntReference source,
            final IMutableIntReference tag, final int maxMessageSize) {
        this.comm = comm;
        this.source = source;
        this.tag = tag;
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
            return false;
        }
        if (request != null) {
            return hasMessage();
        }
        request = comm.Irecv(buffer.byteArray(), 0, buffer.capacity(), MPI.BYTE, source.get(), tag.get());
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (status != null) {
            return true;
        }
        status = request.Test();
        return status != null;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int length = status.Get_count(MPI.BYTE);
        if (ClosedByteBuffer.isClosed(buffer, 0, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        sourceBefore = source.getAndSet(status.source);
        tagBefore = tag.getAndSet(status.tag);
        return buffer.sliceTo(length);
    }

    @Override
    public void readFinished() throws IOException {
        request.finalize();
        status.free();
        request = null;
        status = null;
        source.set(sourceBefore);
        tag.set(tagBefore);
    }

}
