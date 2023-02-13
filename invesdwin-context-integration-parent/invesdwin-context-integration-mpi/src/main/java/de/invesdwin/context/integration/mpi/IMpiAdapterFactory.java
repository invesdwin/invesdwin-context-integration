package de.invesdwin.context.integration.mpi;

public interface IMpiAdapterFactory {

    /**
     * Checks if we are in the correct environment to use this adapter. This will check the mpi.MPI class structure to
     * verify the correct one is loaded. This will be used to pick the correct adapter when multiple are available on
     * the classpath.
     */
    boolean isAvailable();

    /**
     * Creates the actual adapter in a way that does not cause ClassNotFound exceptions in isAvailable.
     */
    IMpiAdapter newInstance();

}
