package de.invesdwin.context.integration.mpi.openmpi;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.error.UnknownArgumentException;
import mpi.MPI;

@Immutable
public final class OpenMpiThreadSupports {

    private OpenMpiThreadSupports() {}

    public static int toMpi(final MpiThreadSupport v) {
        switch (v) {
        case THREAD_SINGLE:
            return MPI.THREAD_SINGLE;
        case THREAD_FUNNELED:
            return MPI.THREAD_FUNNELED;
        case THREAD_SERIALIZED:
            return MPI.THREAD_SERIALIZED;
        case THREAD_MULTIPLE:
            return MPI.THREAD_MULTIPLE;
        default:
            throw UnknownArgumentException.newInstance(MpiThreadSupport.class, v);
        }
    }

    public static MpiThreadSupport fromMpi(final int v) {
        if (v == MPI.THREAD_SINGLE) {
            return MpiThreadSupport.THREAD_SINGLE;
        } else if (v == MPI.THREAD_FUNNELED) {
            return MpiThreadSupport.THREAD_FUNNELED;
        } else if (v == MPI.THREAD_SERIALIZED) {
            return MpiThreadSupport.THREAD_SERIALIZED;
        } else if (v == MPI.THREAD_MULTIPLE) {
            return MpiThreadSupport.THREAD_MULTIPLE;
        } else {
            throw UnknownArgumentException.newInstance(int.class, v);
        }
    }

}
