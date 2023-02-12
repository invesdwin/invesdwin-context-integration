package de.invesdwin.context.integration.mpi.fastmpj;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.Request;
import mpi.Status;

@NotThreadSafe
public class FastMpjSendSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final IIntReference dest;
    private final IIntReference tag;
    private IByteBuffer buffer;
    private Request request;

    public FastMpjSendSynchronousWriter(final IIntReference dest, final IIntReference tag) {
        this.dest = dest;
        this.tag = tag;
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
        request = MPI.COMM_WORLD.Isend(buffer.asNioByteBuffer(), 0, length, MPI.BYTE, dest.get(), tag.get());
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
