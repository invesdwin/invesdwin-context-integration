package de.invesdwin.context.integration.hadoop.mapreduce;

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.io.Writable;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.util.assertions.Assertions;

/**
 * This abstract base class handles the application bootstrap properly. Deactivate bootstrap by overriding the default
 * constructor parameter.
 */
@Immutable
public abstract class AReducer<K1 extends Writable, V1 extends Writable, K2 extends Writable, V2 extends Writable>
extends org.apache.hadoop.mapreduce.Reducer<K1, V1, K2, V2> {

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