package de.invesdwin.context.integration.mpi;

import javax.annotation.concurrent.Immutable;

@Immutable
public enum MpiThreadSupport {
    THREAD_SINGLE,
    THREAD_FUNNELED,
    THREAD_SERIALIZED,
    THREAD_MULTIPLE;
}
