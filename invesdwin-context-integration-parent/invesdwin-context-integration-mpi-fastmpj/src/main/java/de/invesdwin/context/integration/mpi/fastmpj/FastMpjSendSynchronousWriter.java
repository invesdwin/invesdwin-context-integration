package de.invesdwin.context.integration.mpi.fastmpj;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.Intracomm;
import mpi.MPI;
import mpi.Request;
import mpi.Status;

@NotThreadSafe
public class FastMpjSendSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference dest;
    private final IIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;

    public FastMpjSendSynchronousWriter(final Intracomm comm, final IIntReference dest, final IIntReference tag,
            final int maxMessageSize) {
        this.comm = comm;
        this.dest = dest;
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
    public boolean writeReady() throws IOException {
        return true;
    }

    @Override
    public void write(final IByteBufferProvider message) throws IOException {
        final int length = message.getBuffer(buffer);
        request = comm.Isend(buffer.byteArray(), 0, length, MPI.BYTE, dest.get(), tag.get());
    }

    @Override
    public boolean writeFlushed() throws IOException {
        if (request == null) {
            return true;
        } else {
            final Status status = request.Test();
            if (status != null) {
                request.Free();
                request = null;
                return true;
            } else {
                return false;
            }
        }
    }

}
