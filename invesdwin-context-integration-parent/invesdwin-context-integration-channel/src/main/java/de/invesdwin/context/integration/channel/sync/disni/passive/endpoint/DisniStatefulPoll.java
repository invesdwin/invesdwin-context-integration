package de.invesdwin.context.integration.channel.sync.disni.passive.endpoint;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import com.ibm.disni.verbs.SVCPollCq;

@NotThreadSafe
public class DisniStatefulPoll implements Closeable {

    private final SVCPollCq poll;
    private int polls = 0;

    public DisniStatefulPoll(final SVCPollCq poll) {
        this.poll = poll;
    }

    public boolean hasPendingTask() {
        if (polls == 0) {
            try {
                polls += poll.execute().getPolls();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return polls > 0;
    }

    public void finishTask() {
        polls--;
    }

    @Override
    public void close() {
        poll.free();
        polls = 0;
    }

}
