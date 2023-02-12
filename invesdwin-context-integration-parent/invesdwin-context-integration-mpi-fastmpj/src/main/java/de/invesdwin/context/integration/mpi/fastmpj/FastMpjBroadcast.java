package de.invesdwin.context.integration.mpi.fastmpj;

import javax.annotation.concurrent.Immutable;

import mpi.Datatype;
import mpi.MPI;
import mpi.MPIException;
import mpi.Status;

@Immutable
public final class FastMpjBroadcast {

    private static final int BCAST_TAG = 35000;

    private FastMpjBroadcast() {}

    public static Status mstBroadcast(final Object buf, final int offset, final int count, final Datatype type,
            final int root) throws MPIException {
        final int left = 0;
        final int right = MPI.COMM_WORLD.Size() - 1;
        return mstBroadcast(buf, offset, count, type, root, left, right);
    }

    private static Status mstBroadcast(final Object buf, final int offset, final int count, final Datatype type,
            final int root, final int left, final int right) throws MPIException {
        final int dest;
        final int me = MPI.COMM_WORLD.Rank();

        if (left == right) {
            return null;
        }
        final int mid = (left + right) / 2;

        if (root <= mid) {
            dest = right;
        } else {
            dest = left;
        }

        Status status = null;
        if (me == root) {
            MPI.COMM_WORLD.Send(buf, offset, count, type, dest, BCAST_TAG);
        }
        if (me == dest) {
            status = MPI.COMM_WORLD.Recv(buf, offset, count, type, root, BCAST_TAG);
        }
        if (me <= mid && root <= mid) {
            final Status newStatus = mstBroadcast(buf, offset, count, type, root, left, mid);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me <= mid && root > mid) {
            final Status newStatus = mstBroadcast(buf, offset, count, type, dest, left, mid);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me > mid && root <= mid) {
            final Status newStatus = mstBroadcast(buf, offset, count, type, dest, mid + 1, right);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me > mid && root > mid) {
            final Status newStatus = mstBroadcast(buf, offset, count, type, root, mid + 1, right);
            if (newStatus != null) {
                status = newStatus;
            }
        }
        return status;
    }

}
