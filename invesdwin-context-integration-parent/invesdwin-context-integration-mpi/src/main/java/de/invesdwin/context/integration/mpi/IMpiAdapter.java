package de.invesdwin.context.integration.mpi;

import java.io.Closeable;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.reference.integer.IIntReference;
import de.invesdwin.util.concurrent.reference.integer.ImmutableIntReference;
import de.invesdwin.util.streams.buffer.bytes.IByteBufferProvider;

public interface IMpiAdapter extends Closeable {

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

    /**
     * Gets the rank of the calling MPI process in the communicator specified. If the calling MPI process does not
     * belong to the communicator passed, MPI_PROC_NULL is returned.
     */
    int rank();

    /**
     * Gets the number of MPI processes in the communicator in which belongs the calling MPI process.
     */
    int size();

    int anySource();

    int anyTag();

    /**
     * MPI_Barrier blocks all MPI processes in the given communicator until they all call this routine.
     */
    void barrier();

    default ISynchronousWriter<IByteBufferProvider> newBcastWriter(final int root) {
        return newBcastWriter(ImmutableIntReference.of(root));
    }

    /**
     * MPI_Ibcast is the non-blocking counterpart of MPI_Bcast which broadcasts a message from a process to all other
     * processes in the same communicator. This is a collective operation; it must be called by all processes in the
     * communicator. To see the blocking counterpart of MPI_Ibcast, see MPI_Bcast.
     */
    ISynchronousWriter<IByteBufferProvider> newBcastWriter(IIntReference root);

    default ISynchronousReader<IByteBufferProvider> newBcastReader(final int root, final int maxMessageSize) {
        return newBcastReader(ImmutableIntReference.of(root), maxMessageSize);
    }

    /**
     * MPI_Ibcast is the non-blocking counterpart of MPI_Bcast which broadcasts a message from a process to all other
     * processes in the same communicator. This is a collective operation; it must be called by all processes in the
     * communicator. To see the blocking counterpart of MPI_Ibcast, see MPI_Bcast.
     * 
     * The length of the received message must be less than or equal to the length of the receive buffer. An
     * MPI_ERR_TRUNCATE is returned upon the overflow condition.
     */
    ISynchronousReader<IByteBufferProvider> newBcastReader(IIntReference root, int maxMessageSize);

    default ISynchronousWriter<IByteBufferProvider> newSendWriter(final int dest, final int tag) {
        return newSendWriter(ImmutableIntReference.of(dest), ImmutableIntReference.of(tag));
    }

    /**
     * MPI_Isend is the standard non-blocking send (the capital 'I' stands for immediate return). The word standard
     * indicates that this routine is not explicitly told whether to send the message in a synchronous mode or
     * asynchronous mode. Instead, MPI_Isend will make that decision itself; it will issue an asynchronous non-blocking
     * send (MPI_Ibsend) if there is enough space in the buffer attached to MPI (MPI_Buffer_attach) to copy the buffer
     * passed, issuing a synchronous non-blocking send (MPI_Issend) otherwise. Either way, as a non-blocking send,
     * MPI_Isend will not block until the buffer passed is safe to be reused. In other words, the user must not attempt
     * to reuse the buffer after MPI_Isend returns without explicitly checking for MPI_Isend completion (using MPI_Wait
     * or MPI_Test). Other non-blocking sends are MPI_Ibsend, MPI_Issend, MPI_Irsend. Refer to its blocking counterpart,
     * MPI_Send, to understand when the completion is reached.
     */
    ISynchronousWriter<IByteBufferProvider> newSendWriter(IIntReference dest, IIntReference tag);

    default ISynchronousReader<IByteBufferProvider> newRecvReader(final int source, final int tag,
            final int maxMessageSize) {
        return newRecvReader(ImmutableIntReference.of(source), ImmutableIntReference.of(tag), maxMessageSize);
    }

    /**
     * MPI_Irecv stands for MPI Receive with Immediate return; it does not block until the message is received. To know
     * if the message has been received, you must use MPI_Wait or MPI_Test on the MPI_Request filled. To see the
     * blocking counterpart of MPI_Irecv, please refer to MPI_Recv.
     * 
     * The length of the received message must be less than or equal to the length of the receive buffer. An
     * MPI_ERR_TRUNCATE is returned upon the overflow condition.
     */
    ISynchronousReader<IByteBufferProvider> newRecvReader(IIntReference source, IIntReference tag, int maxMessageSize);

    /**
     * MPI_Abort terminates the processes that belong to the communicator passed. When the communicator passed is
     * MPI_COMM_WORLD, it is equivalent to shutting down the entire MPI application. (If the communicator passed is a
     * subset of MPI_COMM_WORLD, it may not be possible for an MPI implementation to terminate only the processes
     * belonging to this communicator. In which case, all processes are terminated instead.) The MPI standard stipulates
     * no directive with regard to how the error code passed should be handled. In a UNIX or POSIX environment however,
     * the error code given "should" become the value returned by the main program.
     */
    void abort(int errorCode);

    /**
     * Calls MPI.finalize()
     */
    @Override
    void close();

}