package de.invesdwin.context.integration.mpi.openmpi;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.streams.buffer.bytes.ByteBuffers;
import de.invesdwin.util.streams.buffer.bytes.IByteBuffer;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;
import mpi.MPI;
import mpi.MPIException;
import mpi.Request;

@NotThreadSafe
public class OpenMpiRecvSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final IIntReference source;
    private final IIntReference tag;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;

    public OpenMpiRecvSynchronousReader(final IIntReference source, final IIntReference tag, final int maxMessageSize) {
        this.source = source;
        this.tag = tag;
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
        if (request != null) {
            return hasMessage();
        }
        try {
            request = MPI.COMM_WORLD.iRecv(buffer.nioByteBuffer(), buffer.capacity(), MPI.BYTE, source.get(),
                    tag.get());
        } catch (final MPIException e) {
            throw new IOException(e);
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        try {
            return request.test();
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        try {
            final int length = request.getStatus().getCount(MPI.BYTE);
            return buffer.sliceTo(length);
        } catch (final MPIException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void readFinished() throws IOException {
        try {
            request.free();
        } catch (final MPIException e) {
            throw new IOException(e);
        }
        request = null;
    }

}