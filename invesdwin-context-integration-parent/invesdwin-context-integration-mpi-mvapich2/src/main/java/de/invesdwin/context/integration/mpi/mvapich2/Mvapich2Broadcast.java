package de.invesdwin.context.integration.mpi.mvapich2;

import javax.annotation.concurrent.Immutable;

import mpi.Datatype;
import mpi.Intracomm;
import mpi.MPIException;
import mpi.Status;

@Immutable
public final class Mvapich2Broadcast {

    private static final int BCAST_TAG = 35000;

    private Mvapich2Broadcast() {}

    public static Status mstBroadcast(final Intracomm comm, final Object buf, final int count, final Datatype type,
            final int root) throws MPIException {
        final int left = 0;
        final int right = comm.getSize() - 1;
        return mstBroadcast(comm, buf, count, type, root, left, right);
    }

    private static Status mstBroadcast(final Intracomm comm, final Object buf, final int count, final Datatype type,
            final int root, final int left, final int right) throws MPIException {
        final int dest;
        final int me = comm.getRank();

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
            comm.send(buf, count, type, dest, BCAST_TAG);
        }
        if (me == dest) {
            status = comm.recv(buf, count, type, root, BCAST_TAG);
        }
        if (me <= mid && root <= mid) {
            final Status newStatus = mstBroadcast(comm, buf, count, type, root, left, mid);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me <= mid && root > mid) {
            final Status newStatus = mstBroadcast(comm, buf, count, type, dest, left, mid);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me > mid && root <= mid) {
            final Status newStatus = mstBroadcast(comm, buf, count, type, dest, mid + 1, right);
            if (newStatus != null) {
                status = newStatus;
            }
        } else if (me > mid && root > mid) {
            final Status newStatus = mstBroadcast(comm, buf, count, type, root, mid + 1, right);
            if (newStatus != null) {
                status = newStatus;
            }
        }
        return status;
    }

}
