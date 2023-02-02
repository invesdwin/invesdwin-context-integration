package de.invesdwin.context.integration.channel.sync.spinwait;

import java.util.function.Supplier;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.concurrent.pool.AgronaObjectPool;
import de.invesdwin.util.concurrent.pool.IObjectPool;

@ThreadSafe
public final class SynchronousReaderSpinWaitPool implements IObjectPool<SynchronousReaderSpinWait> {

    public static final SynchronousReaderSpinWaitPool INSTANCE = new SynchronousReaderSpinWaitPool();

    private static volatile Supplier<? extends SynchronousReaderSpinWait> factory = () -> new SynchronousReaderSpinWait();
    private static final AgronaObjectPool<SynchronousReaderSpinWait> POOL = new AgronaObjectPool<SynchronousReaderSpinWait>(
            () -> factory.get()) {
        @Override
        public void invalidateObject(final SynchronousReaderSpinWait element) {
            element.setReader(null);
        }
    };

    private SynchronousReaderSpinWaitPool() {}

    /**
     * This allows to override the default timings of the spinwait.
     */
    public static void setFactory(final Supplier<? extends SynchronousReaderSpinWait> factory) {
        SynchronousReaderSpinWaitPool.factory = factory;
    }

    public static Supplier<? extends SynchronousReaderSpinWait> getFactory() {
        return factory;
    }

    @Override
    public SynchronousReaderSpinWait borrowObject() {
        return POOL.borrowObject();
    }

    @Override
    public void returnObject(final SynchronousReaderSpinWait element) {
        POOL.returnObject(element);
    }

    @Override
    public void clear() {
        POOL.clear();
    }

    @Override
    public void invalidateObject(final SynchronousReaderSpinWait element) {
        POOL.invalidateObject(element);
    }

}
