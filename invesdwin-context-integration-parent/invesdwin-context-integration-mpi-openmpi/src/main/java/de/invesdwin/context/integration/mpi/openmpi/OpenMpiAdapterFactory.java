package de.invesdwin.context.integration.mpi.openmpi;

import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.IMpiAdapterFactory;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public class OpenMpiAdapterFactory implements IMpiAdapterFactory {

    @Override
    public boolean isAvailable() {
        try {
            final Class<Object> mpiClass = Reflections.classForName("mpi.MPI");
            final Method finalizeJniMethod = Reflections.findMethod(mpiClass, "Finalize_jni");
            return finalizeJniMethod != null;
        } catch (final Throwable t) {
            Err.process(t);
            return false;
        }
    }

    @Override
    public IMpiAdapter newInstance() {
        return new de.invesdwin.context.integration.mpi.openmpi.OpenMpiAdapter();
    }

}
