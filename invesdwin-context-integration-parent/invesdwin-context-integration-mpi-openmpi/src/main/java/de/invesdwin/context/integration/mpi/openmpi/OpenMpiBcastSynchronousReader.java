package de.invesdwin.context.integration.mpi.openmpi;

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
import mpi.MPIException;
import mpi.Request;

@NotThreadSafe
public class OpenMpiBcastSynchronousReader implements ISynchronousReader<IByteBufferProvider> {

    private final Intracomm comm;
    private final IIntReference root;
    private final int maxMessageSize;
    private IByteBuffer buffer;
    private Request request;
    private boolean status;

    public OpenMpiBcastSynchronousReader(final Intracomm comm, final IIntReference root, final int maxMessageSize) {
        this.comm = comm;
        this.root = root;
        this.maxMessageSize = maxMessageSize;
    }

    @Override
    public void open() throws IOException {
        buffer = ByteBuffers.allocateDirect(OpenMpiAdapter.MESSAGE_INDEX + maxMessageSize);
    }

    @Override
    public void close() throws IOException {
        buffer = null;
        request = null;
        status = false;
    }

    @Override
    public boolean hasNext() throws IOException {
        if (buffer == null) {
            throw FastEOFException.getInstance("closed");
        }
        if (request != null) {
            return hasMessage();
        }
        try {
            request = comm.iBcast(buffer.nioByteBuffer(), buffer.capacity(), MPI.BYTE, root.get());
        } catch (final MPIException e) {
            throw new IOException(e);
        }
        return hasMessage();
    }

    private boolean hasMessage() throws IOException {
        if (status) {
            return true;
        }
        try {
            status = request.test();
            return status;
        } catch (final MPIException e) {
            throw new IOException(e);
        }
    }

    @Override
    public IByteBufferProvider readMessage() throws IOException {
        final int length = buffer.getInt(OpenMpiAdapter.SIZE_INDEX);
        if (ClosedByteBuffer.isClosed(buffer, OpenMpiAdapter.MESSAGE_INDEX, length)) {
            close();
            throw FastEOFException.getInstance("closed by other side");
        }
        return buffer.slice(OpenMpiAdapter.MESSAGE_INDEX, length);
    }

    @Override
    public void readFinished() throws IOException {
        try {
            request.free();
        } catch (final MPIException e) {
            throw new IOException(e);
        }
        status = false;
        request = null;
    }

}
