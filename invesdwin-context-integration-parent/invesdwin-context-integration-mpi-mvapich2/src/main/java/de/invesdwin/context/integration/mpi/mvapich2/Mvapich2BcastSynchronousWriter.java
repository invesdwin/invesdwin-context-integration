package de.invesdwin.context.integration.mpi.mvapich2;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;

@NotThreadSafe
public class Mvapich2BcastSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference root;
    private final int maxMessageSize;
    private IByteBuffer buffer;

    public Mvapich2BcastSynchronousWriter(final Intracomm comm, final IIntReference root, final int maxMessageSize) {
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
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int length = message.getBuffer(buffer);
        Mvapich2Broadcast.mstBroadcast(comm, buffer, length, MPI.BYTE, root.get());
    }

    @Override
    public boolean writeFlushed() throws IOException {
        return true;
    }

}
