package de.invesdwin.context.integration.channel.sync.spinwait;

import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousReader;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;

@SuppressWarnings({ "unchecked", "rawtypes" })
@ThreadSafe
public final class SynchronousReaderSpinWaitPool {

    private static volatile Function<MutableDelegateSynchronousReader<?>, ? extends SynchronousReaderSpinWait> factory = (
            t) -> new SynchronousReaderSpinWait(t);
    private static final AgronaObjectPool<SynchronousReaderSpinWait> POOL = new AgronaObjectPool<SynchronousReaderSpinWait>(
            () -> factory.apply(new MutableDelegateSynchronousReader<>())) {
        @Override
        public void invalidateObject(final SynchronousReaderSpinWait element) {
            final MutableDelegateSynchronousReader<?> holder = (MutableDelegateSynchronousReader<?>) element
                    .getReader();
            holder.setDelegate(null);
        }
    };

    private SynchronousReaderSpinWaitPool() {}

    /**
     * This allows to override the default timings of the spinwait.
     */
    public static void setFactory(
            final Function<MutableDelegateSynchronousReader<?>, ? extends SynchronousReaderSpinWait> factory) {
        SynchronousReaderSpinWaitPool.factory = factory;
    }

    public static Function<MutableDelegateSynchronousReader<?>, ? extends SynchronousReaderSpinWait> getFactory() {
        return factory;
    }

    public static <M> SynchronousReaderSpinWait<M> borrowObject(final ISynchronousReader<M> reader) {
        final SynchronousReaderSpinWait spinWait = POOL.borrowObject();
        final MutableDelegateSynchronousReader holder = (MutableDelegateSynchronousReader) spinWait.getReader();
        holder.setDelegate(reader);
        return spinWait;
    }

    public static void returnObject(final SynchronousReaderSpinWait<?> element) {
        POOL.returnObject(element);
    }

    public static void clear() {
        POOL.clear();
    }

    public static void invalidateObject(final SynchronousReaderSpinWait<?> element) {
        POOL.invalidateObject(element);
    }

}
