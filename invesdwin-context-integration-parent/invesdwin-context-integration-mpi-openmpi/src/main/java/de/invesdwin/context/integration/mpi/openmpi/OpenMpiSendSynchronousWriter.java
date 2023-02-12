package de.invesdwin.context.integration.mpi.openmpi;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

@NotThreadSafe
public class OpenMpiSendSynchronousWriter implements ISynchronousWriter<IByteBufferProvider> {

    private final IIntReference dest;
    private final IIntReference tag;
    private IByteBuffer buffer;
    private Request request;

    public OpenMpiSendSynchronousWriter(final IIntReference dest, final IIntReference tag) {
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
        try {
            request = MPI.COMM_WORLD.iSend(buffer.nioByteBuffer(), length, MPI.BYTE, dest.get(), tag.get());
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean writeFlushed() throws IOException {
        try {
            if (request == null) {
                return true;
            } else if (request.test()) {
                request.free();
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
