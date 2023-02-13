package de.invesdwin.context.integration.mpi.openmpi;

import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.IMpiAdapterFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public class OpenMpiAdapterFactory implements IMpiAdapterFactory {

    @Override
    public boolean isAvailable() {
        try {
            final Class<Object> mpiClass = Reflections.classForName("mpi.MPI");
            final Field finalizedField = Reflections.findField(mpiClass, "finalized");
            return finalizedField != null;
        } catch (final Throwable t) {
            return false;
        }
    }

    @Override
    public IMpiAdapter newInstance() {
        return new de.invesdwin.context.integration.mpi.openmpi.OpenMpiAdapter();
    }

}
