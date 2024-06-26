package de.invesdwin.context.integration.channel.sync.queue.blocking;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.integration.channel.sync.ISynchronousChannel;
import de.invesdwin.context.integration.channel.sync.command.EmptySynchronousCommand;
import de.invesdwin.util.concurrent.Executors;
import de.invesdwin.util.concurrent.WrappedExecutorService;
import de.invesdwin.util.concurrent.reference.EmptyReference;
import de.invesdwin.util.concurrent.reference.IReference;
import de.invesdwin.util.math.random.IRandomGenerator;
import de.invesdwin.util.math.random.PseudoRandomGenerators;

/**
 * WARNING: can cause cpu spikes
 */
@Deprecated
@NotThreadSafe
public abstract class ABlockingQueueSynchronousChannel<M> implements ISynchronousChannel {

    public static final WrappedExecutorService CLOSED_SYNCHRONIZER = Executors
            .newCachedThreadPool(ABlockingQueueSynchronousChannel.class.getSimpleName() + "_closedSynchronizer");

    protected BlockingQueue<IReference<M>> queue;

    @SuppressWarnings("unchecked")
    public ABlockingQueueSynchronousChannel(final BlockingQueue<? extends IReference<M>> queue) {
        this.queue = (BlockingQueue<IReference<M>>) queue;
    }

    @Override
    public void open() throws IOException {}

    @Override
    public void close() throws IOException {
        if (queue != null) {
            sendClosedMessage();
            queue = null;
        }
    }

    protected void sendClosedMessage() {
        final BlockingQueue<IReference<M>> queueCopy = queue;
        CLOSED_SYNCHRONIZER.execute(() -> {
            //randomize sleeps to increase chance of meeting each other
            final IRandomGenerator random = PseudoRandomGenerators.getThreadLocalPseudoRandom();
            try {
                boolean closedMessageSent = false;
                boolean closedMessageReceived = false;
                while (!closedMessageReceived || !closedMessageSent) {
                    if (queueCopy.poll(random.nextInt(2), TimeUnit.MILLISECONDS) == EmptySynchronousCommand
                            .getInstance()) {
                        closedMessageReceived = true;
                    }
                    if (queueCopy.offer(newEmptyReference(), random.nextInt(2), TimeUnit.MILLISECONDS)) {
                        closedMessageSent = true;
                    }
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

    }

    protected IReference<M> newEmptyReference() {
        return EmptyReference.getInstance();
    }

}
