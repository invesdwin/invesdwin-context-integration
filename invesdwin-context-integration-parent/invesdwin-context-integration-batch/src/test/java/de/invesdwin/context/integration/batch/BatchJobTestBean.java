package de.invesdwin.context.integration.batch;

import javax.annotation.concurrent.Immutable;
import jakarta.inject.Named;

@Immutable
@Named
public class BatchJobTestBean {

    public boolean test() {
        return true;
    }

}
