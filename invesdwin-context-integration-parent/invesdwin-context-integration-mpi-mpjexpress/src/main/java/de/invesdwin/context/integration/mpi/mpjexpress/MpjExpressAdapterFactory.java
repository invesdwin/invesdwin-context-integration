package de.invesdwin.context.integration.mpi.mpjexpress;

import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.integration.mpi.IMpiAdapter;
import de.invesdwin.context.integration.mpi.IMpiAdapterFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public class MpjExpressAdapterFactory implements IMpiAdapterFactory {

    @Override
    public boolean isAvailable() {
        try {
            final Class<Object> mpiClass = Reflections.classForName("mpi.MPI");
            final Field loggerField = Reflections.findField(mpiClass, "logger");
            return loggerField != null;
        } catch (final Throwable t) {
            return false;
        }
    }

    @Override
    public IMpiAdapter newInstance() {
        return new de.invesdwin.context.integration.mpi.mpjexpress.MpjExpressAdapter();
    }

}
