package de.invesdwin.context.integration.channel.sync.hadronio.netty;

import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.hadronio.HadronioSocketSynchronousChannel;
import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.EventLoopTaskQueueFactory;
import io.netty.channel.SelectStrategyFactory;
import io.netty.channel.SingleThreadEventLoop;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.EventExecutorChooserFactory;
import io.netty.util.concurrent.RejectedExecutionHandler;
import io.netty.util.concurrent.RejectedExecutionHandlers;

@NotThreadSafe
public class HadroNioEventLoopGroup extends NioEventLoopGroup {

    /**
     * Create a new instance using the default number of threads, the default {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public HadroNioEventLoopGroup() {
        this(0);
    }

    /**
     * Create a new instance using the specified number of threads, {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public HadroNioEventLoopGroup(final int nThreads) {
        this(nThreads, (Executor) null);
    }

    /**
     * Create a new instance using the default number of threads, the given {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public HadroNioEventLoopGroup(final ThreadFactory threadFactory) {
        this(0, threadFactory, HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

    /**
     * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the
     * {@link SelectorProvider} which is returned by {@link SelectorProvider#provider()}.
     */
    public HadroNioEventLoopGroup(final int nThreads, final ThreadFactory threadFactory) {
        this(nThreads, threadFactory, HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor) {
        this(nThreads, executor, HadronioSocketSynchronousChannel.HADRONIO_PROVIDER);
    }

    /**
     * Create a new instance using the specified number of threads, the given {@link ThreadFactory} and the given
     * {@link SelectorProvider}.
     */
    public HadroNioEventLoopGroup(final int nThreads, final ThreadFactory threadFactory,
            final SelectorProvider selectorProvider) {
        this(nThreads, threadFactory, selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
    }

    public HadroNioEventLoopGroup(final int nThreads, final ThreadFactory threadFactory,
            final SelectorProvider selectorProvider, final SelectStrategyFactory selectStrategyFactory) {
        super(nThreads, threadFactory, selectorProvider, selectStrategyFactory);
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor,
            final SelectorProvider selectorProvider) {
        this(nThreads, executor, selectorProvider, DefaultSelectStrategyFactory.INSTANCE);
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor, final SelectorProvider selectorProvider,
            final SelectStrategyFactory selectStrategyFactory) {
        super(nThreads, executor, selectorProvider, selectStrategyFactory);
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor,
            final EventExecutorChooserFactory chooserFactory, final SelectorProvider selectorProvider,
            final SelectStrategyFactory selectStrategyFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory,
                RejectedExecutionHandlers.reject());
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor,
            final EventExecutorChooserFactory chooserFactory, final SelectorProvider selectorProvider,
            final SelectStrategyFactory selectStrategyFactory,
            final RejectedExecutionHandler rejectedExecutionHandler) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory, rejectedExecutionHandler);
    }

    public HadroNioEventLoopGroup(final int nThreads, final Executor executor,
            final EventExecutorChooserFactory chooserFactory, final SelectorProvider selectorProvider,
            final SelectStrategyFactory selectStrategyFactory, final RejectedExecutionHandler rejectedExecutionHandler,
            final EventLoopTaskQueueFactory taskQueueFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory, rejectedExecutionHandler,
                taskQueueFactory);
    }

    /**
     * @param nThreads
     *            the number of threads that will be used by this instance.
     * @param executor
     *            the Executor to use, or {@code null} if default one should be used.
     * @param chooserFactory
     *            the {@link EventExecutorChooserFactory} to use.
     * @param selectorProvider
     *            the {@link SelectorProvider} to use.
     * @param selectStrategyFactory
     *            the {@link SelectStrategyFactory} to use.
     * @param rejectedExecutionHandler
     *            the {@link RejectedExecutionHandler} to use.
     * @param taskQueueFactory
     *            the {@link EventLoopTaskQueueFactory} to use for {@link SingleThreadEventLoop#execute(Runnable)}, or
     *            {@code null} if default one should be used.
     * @param tailTaskQueueFactory
     *            the {@link EventLoopTaskQueueFactory} to use for
     *            {@link SingleThreadEventLoop#executeAfterEventLoopIteration(Runnable)}, or {@code null} if default one
     *            should be used.
     */
    public HadroNioEventLoopGroup(final int nThreads, final Executor executor,
            final EventExecutorChooserFactory chooserFactory, final SelectorProvider selectorProvider,
            final SelectStrategyFactory selectStrategyFactory, final RejectedExecutionHandler rejectedExecutionHandler,
            final EventLoopTaskQueueFactory taskQueueFactory, final EventLoopTaskQueueFactory tailTaskQueueFactory) {
        super(nThreads, executor, chooserFactory, selectorProvider, selectStrategyFactory, rejectedExecutionHandler,
                taskQueueFactory, tailTaskQueueFactory);
    }

}
