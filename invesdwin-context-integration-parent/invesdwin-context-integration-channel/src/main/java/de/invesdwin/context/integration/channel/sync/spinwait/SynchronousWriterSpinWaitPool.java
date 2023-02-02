package de.invesdwin.context.integration.channel.sync.spinwait;

import java.util.function.Function;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousWriter;
import de.invesdwin.util.concurrent.pool.AgronaObjectPool;

@SuppressWarnings({ "rawtypes", "unchecked" })
@ThreadSafe
public final class SynchronousWriterSpinWaitPool {

    private static volatile Function<MutableDelegateSynchronousWriter<?>, ? extends SynchronousWriterSpinWait> factory = (
            t) -> new SynchronousWriterSpinWait(t);
    private static final AgronaObjectPool<SynchronousWriterSpinWait> POOL = new AgronaObjectPool<SynchronousWriterSpinWait>(
            () -> factory.apply(new MutableDelegateSynchronousWriter<>())) {
        @Override
        public void invalidateObject(final SynchronousWriterSpinWait element) {
            final MutableDelegateSynchronousWriter<?> holder = (MutableDelegateSynchronousWriter<?>) element
                    .getWriter();
            holder.setDelegate(null);
        }
    };

    private SynchronousWriterSpinWaitPool() {}

    /**
     * This allows to override the default timings of the spinwait.
     */
    public static void setFactory(
            final Function<MutableDelegateSynchronousWriter<?>, ? extends SynchronousWriterSpinWait> factory) {
        SynchronousWriterSpinWaitPool.factory = factory;
    }

    public static Function<MutableDelegateSynchronousWriter<?>, ? extends SynchronousWriterSpinWait> getFactory() {
        return factory;
    }

    public static <M> SynchronousWriterSpinWait<M> borrowObject(final ISynchronousWriter<M> writer) {
        final SynchronousWriterSpinWait spinWait = POOL.borrowObject();
        final MutableDelegateSynchronousWriter holder = (MutableDelegateSynchronousWriter) spinWait.getWriter();
        holder.setDelegate(writer);
        return spinWait;
    }

    public static void returnObject(final SynchronousWriterSpinWait<?> element) {
        POOL.returnObject(element);
    }

    public static void clear() {
        POOL.clear();
    }

    public static void invalidateObject(final SynchronousWriterSpinWait<?> element) {
        POOL.invalidateObject(element);
    }

}
