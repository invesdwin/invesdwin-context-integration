package de.invesdwin.context.integration.channel.sync.spinwait;

import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;

@ThreadSafe
public final class SynchronousWriterSpinWaitPool implements IObjectPool<SynchronousWriterSpinWait> {

    public static final SynchronousWriterSpinWaitPool INSTANCE = new SynchronousWriterSpinWaitPool();

    private static volatile Supplier<? extends SynchronousWriterSpinWait> factory = () -> new SynchronousWriterSpinWait();
    private static final AgronaObjectPool<SynchronousWriterSpinWait> POOL = new AgronaObjectPool<SynchronousWriterSpinWait>(
            () -> factory.get()) {
        @Override
        public void invalidateObject(final SynchronousWriterSpinWait element) {
            element.setWriter(null);
        }
    };

    private SynchronousWriterSpinWaitPool() {}

    /**
     * This allows to override the default timings of the spinwait.
     */
    public static void setFactory(final Supplier<? extends SynchronousWriterSpinWait> factory) {
        SynchronousWriterSpinWaitPool.factory = factory;
    }

    public static Supplier<? extends SynchronousWriterSpinWait> getFactory() {
        return factory;
    }

    @Override
    public SynchronousWriterSpinWait borrowObject() {
        return POOL.borrowObject();
    }

    @Override
    public void returnObject(final SynchronousWriterSpinWait element) {
        POOL.returnObject(element);
    }

    @Override
    public void clear() {
        POOL.clear();
    }

    @Override
    public void invalidateObject(final SynchronousWriterSpinWait element) {
        POOL.invalidateObject(element);
    }

}
