package de.invesdwin.common.batch;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;

@Immutable
@Named
public class BatchJobTestBean {

    public boolean test() {
        return true;
    }

}
