package de.invesdwin.context.integration.mpi;

import java.io.Closeable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IMpiSynchronousChannelFactory extends Closeable {

    default void init(final String[] args) {
        initThread(args, MpiThreadSupport.THREAD_SINGLE);
    }

    /**
     * MPI_Init_thread initialises the MPI environment like MPI_Init does, except that the former also explicitly
     * indicates the level of multithreading support needed: MPI_THREAD_SINGLE, MPI_THREAD_FUNNELED,
     * MPI_THREAD_SERIALIZED or MPI_THREAD_MULTIPLE. MPI_Init is equivalent to MPI_Init_thread with the
     * MPI_THREAD_SINGLE thread support level. The routine MPI_Init_thread must be called by each MPI process, once and
     * before any other MPI routine.
     */
    MpiThreadSupport initThread(String[] args, MpiThreadSupport threadRequirement);

    ISynchronousWriter<IByteBufferProvider> newBcast();

    ISynchronousWriter<IByteBufferProvider> newSend();

    ISynchronousReader<IByteBufferProvider> newReceive();

    /**
     * Calls MPI.finalize()
     */
    @Override
    void close();

}
