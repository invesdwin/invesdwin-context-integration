package de.invesdwin.context.integration.batch;

import java.util.Set;

import javax.annotation.concurrent.Immutable;
import jakarta.inject.Named;

@Named
@Immutable
public class DisabledBatchContextSupport implements IDisabledBatchContext {

    @Override
    public Set<String> getResourceNames() {
        return null;
    }

}
