package de.invesdwin.context.integration.channel.rpc.base.server.async.poll;

import javax.annotation.concurrent.ThreadSafe;

import org.agrona.concurrent.ManyToOneConcurrentLinkedQueue;

import de.invesdwin.context.integration.channel.rpc.base.server.service.command.serializing.EagerSerializingServiceSynchronousCommand;
import de.invesdwin.context.integration.channel.rpc.base.server.session.result.ProcessResponseResult;
import de.invesdwin.util.collections.iterable.buffer.NodeBufferingIterator;

@ThreadSafe
public class SyncPollingQueueProvider implements IPollingQueueProvider {

    private final ManyToOneConcurrentLinkedQueue<ProcessResponseResult> pollingQueueAdds = new ManyToOneConcurrentLinkedQueue<>();
    private final NodeBufferingIterator<ProcessResponseResult> pollingQueue = new NodeBufferingIterator<>();

    public SyncPollingQueueProvider() {}

    @Override
    public void addToPollingQueue(final ProcessResponseResult result) {
        pollingQueueAdds.add(result);
    }

    public boolean maybePollResults() {
        boolean changed = false;
        if (!pollingQueueAdds.isEmpty()) {
            ProcessResponseResult addPollingResult = pollingQueueAdds.poll();
            while (addPollingResult != null) {
                pollingQueue.add(addPollingResult);
                changed = true;
                addPollingResult = pollingQueueAdds.poll();
            }
        }
        if (!pollingQueue.isEmpty()) {
            ProcessResponseResult pollingResult = pollingQueue.getHead();
            while (pollingResult != null) {
                final ProcessResponseResult nextPollingResult = pollingResult.getNext();
                if (pollingResult.isDone()) {
                    if (pollingResult.isDelayedWriteResponse()) {
                        final EagerSerializingServiceSynchronousCommand<Object> response = pollingResult.getResponse();
                        if (response.hasMessage()) {
                            pollingResult.getContext().write(response.asBuffer());
                        }
                    }
                    pollingQueue.remove(pollingResult);
                    changed = true;
                    pollingResult.close();
                }
                pollingResult = nextPollingResult;
            }
        }
        return changed;
    }

}
