package de.invesdwin.context.integration.mpi.mpjexpress;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;

@NotThreadSafe
public class MpjExpressBcastSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final IIntReference root;
    private IByteBuffer buffer;

    public MpjExpressBcastSynchronousWriter(final IIntReference root) {
        this.root = root;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirectExpandable();
    }

    @Override
    public void close() throws IOException {
        buffer = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int length = message.getBuffer(buffer);
        MPI.COMM_WORLD.Bcast(buffer.asNioByteBuffer(), 0, length, MPI.BYTE, root.get());
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
