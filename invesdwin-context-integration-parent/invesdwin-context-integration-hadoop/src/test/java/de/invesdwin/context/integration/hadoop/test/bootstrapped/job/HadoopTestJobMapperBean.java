package de.invesdwin.context.integration.hadoop.test.bootstrapped.job;

import javax.annotation.concurrent.Immutable;

import jakarta.inject.Named;

@Named
@Immutable
public class HadoopTestJobMapperBean {

    public boolean test() {
        return true;
    }

}
