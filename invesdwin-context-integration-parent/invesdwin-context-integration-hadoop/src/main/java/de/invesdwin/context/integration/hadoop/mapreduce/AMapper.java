package de.invesdwin.context.integration.hadoop.mapreduce;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.util.assertions.Assertions;

/**
 * This abstract base class handles the application bootstrap properly. Deactivate bootstrap by overriding the default
 * constructor parameter.
 */
@Immutable
public abstract class AMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends org.apache.hadoop.mapreduce.Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    static {
        Assertions.assertThat(PreMergedContext.getInstance()).isNotNull();
    }

    public AMapper(final boolean bootstrap) {
        if (bootstrap) {
            MergedContext.autowire(this);
        }
    }

    public AMapper() {
        this(true);
    }

}
