package de.invesdwin.context.integration.mpi.mvapich2;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.ClosedByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

@NotThreadSafe
public class Mvapich2SendSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference dest;
    private final IIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;

    public Mvapich2SendSynchronousWriter(final Intracomm comm, final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        this.comm = comm;
        this.dest = dest;
        this.tag = tag;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirect(Mvapich2Adapter.MESSAGE_INDEX + maxMessageSize);
    }

    @Override
    public void close() throws IOException {
        if (buffer != null) {
            write(ClosedByteBuffer.INSTANCE);
            buffer = null;
        }
        request = null;
    }

    @Override
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int length = message.getBuffer(buffer.sliceFrom(Mvapich2Adapter.MESSAGE_INDEX));
        buffer.putInt(Mvapich2Adapter.SIZE_INDEX, length);
        try {
            request = comm.iSend(buffer.nioByteBuffer(), Mvapich2Adapter.MESSAGE_INDEX + length, MPI.BYTE, dest.get(),
                    tag.get());
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        try {
            if (request == null) {
                return true;
            }
            if (request.test()) {
                request = null;
                return true;
            } else {
                return false;
            }
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

}
