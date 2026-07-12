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
public abstract class AReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
        extends org.apache.hadoop.mapreduce.Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

    static {
        Assertions.assertThat(PreMergedContext.getInstance()).isNotNull();
    }

    public AReducer(final boolean bootstrap) {
        if (bootstrap) {
            MergedContext.autowire(this);
        }
    }

    public AReducer() {
        this(true);
    }

}