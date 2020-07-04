package de.invesdwin.context.integration.jppf.topology;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;

import javax.annotation.concurrent.Immutable;

import org.jppf.node.protocol.TaskBundle;
import org.jppf.node.protocol.TaskExecutionDispatcher;

import de.invesdwin.util.lang.reflection.Reflections;

@Immutable
public final class TaskExecutionDispatchers {

    private static final MethodHandle TASKEXECUTIONDISPATCHER_BUNDLE_GETTER;

    static {
        final Field bundleField = Reflections.findField(TaskExecutionDispatcher.class, "bundle");
        Reflections.makeAccessible(bundleField);
        try {
            TASKEXECUTIONDISPATCHER_BUNDLE_GETTER = MethodHandles.lookup().unreflectGetter(bundleField);
        } catch (final IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private TaskExecutionDispatchers() {}

    public static TaskBundle getTaskBundle(final TaskExecutionDispatcher executionDisptacher) {
        try {
            return (TaskBundle) TASKEXECUTIONDISPATCHER_BUNDLE_GETTER.invoke(executionDisptacher);
        } catch (final Throwable e) {
            throw new RuntimeException(e);
        }
    }

}
