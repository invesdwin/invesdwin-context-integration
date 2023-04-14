package de.invesdwin.context.integration.mpi.mvapich2;

import java.lang.reflect.Method;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.IMpiAdapterFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public class Mvapich2AdapterFactory implements IMpiAdapterFactory {

    @Override
    public boolean isAvailable() {
        try {
            final Class<Object> mpiClass = Reflections.classForName("mpi.MPI");
            final Method nativeFinishMethod = Reflections.findMethod(mpiClass, "nativeFinish");
            return nativeFinishMethod != null;
        } catch (final Throwable t) {
            return false;
        }
    }

    @Override
    public IMpiAdapter newInstance() {
        return new de.invesdwin.context.integration.mpi.mvapich2.Mvapich2Adapter();
    }

}
