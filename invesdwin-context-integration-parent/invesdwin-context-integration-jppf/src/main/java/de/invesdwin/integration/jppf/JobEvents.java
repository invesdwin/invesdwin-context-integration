package de.invesdwin.integration.jppf;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

import org.jppf.client.event.JobEvent;
import org.jppf.execute.ExecutorChannel;

import de.invesdwin.util.lang.Reflections;

@Immutable
public final class JobEvents {

    private static final MethodHandle JOBEVENT_CHANNEL_GETTER;

    static {
        try {
            final Field channelField = Reflections.findField(JobEvent.class, "channel");
            Reflections.makeAccessibleFinal(channelField);
            JOBEVENT_CHANNEL_GETTER = MethodHandles.lookup().unreflectGetter(channelField);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private JobEvents() {}

    public static ExecutorChannel<?> extractChannel(final JobEvent event) {
        try {
            final ExecutorChannel<?> channel = (ExecutorChannel<?>) JOBEVENT_CHANNEL_GETTER.invokeExact(event);
            return channel;
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
