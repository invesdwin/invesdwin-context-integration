package de.invesdwin.context.integration.mpi.fastmpj;

import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.IMpiAdapterFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public class FastMpjAdapterFactory implements IMpiAdapterFactory {

    @Override
    public boolean isAvailable() {
        try {
            final Class<Object> mpiClass = Reflections.classForName("mpi.MPI");
            //finalized field is called j here due to obfuscation
            final Field jField = Reflections.findField(mpiClass, "j");
            return jField != null;
        } catch (final Throwable t) {
            return false;
        }
    }

    @Override
    public IMpiAdapter newInstance() {
        return new de.invesdwin.context.integration.mpi.fastmpj.FastMpjAdapter();
    }

}
