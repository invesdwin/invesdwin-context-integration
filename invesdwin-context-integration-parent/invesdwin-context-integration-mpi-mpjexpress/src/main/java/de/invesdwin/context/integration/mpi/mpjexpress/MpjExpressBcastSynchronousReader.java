package de.invesdwin.context.integration.mpi.mpjexpress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.Status;

@NotThreadSafe
public class MpjExpressBcastSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final IIntReference root;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Status status;

    public MpjExpressBcastSynchronousReader(final IIntReference root, final int maxMessageSize) {
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
        return true;
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        status = MpjExpressBroadcast.mstBroadcast(buffer.byteArray(), 0, buffer.capacity(), MPI.BYTE, root.get());
        final int length = status.Get_count(MPI.BYTE);
        return buffer.sliceTo(length);
    }

    @Override
    public void readFinished() throws IOException {
        status.free();
        status = null;
    }

}
