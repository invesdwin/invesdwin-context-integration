package de.invesdwin.context.integration.mpi.fastmpj;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.MpiThreadSupport;
import de.invesdwin.util.error.UnknownArgumentException;
import mpi.MPI;

@Immutable
public final class MpiThreadSupports {

    private MpiThreadSupports() {}

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
        switch (v) {
        case MPI.THREAD_SINGLE:
            return MpiThreadSupport.THREAD_SINGLE;
        case MPI.THREAD_FUNNELED:
            return MpiThreadSupport.THREAD_FUNNELED;
        case MPI.THREAD_SERIALIZED:
            return MpiThreadSupport.THREAD_SERIALIZED;
        case MPI.THREAD_MULTIPLE:
            return MpiThreadSupport.THREAD_MULTIPLE;
        default:
            throw UnknownArgumentException.newInstance(int.class, v);
        }
    }

}
