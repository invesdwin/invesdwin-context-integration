package de.invesdwin.integration.jppf.notification;

import javax.annotation.concurrent.Immutable;
import javax.management.Notification;
import javax.management.NotificationListener;

import org.jppf.management.TaskExecutionNotification;

/**
 * http://www.jppf.org/doc/5.2/index.php?title=Task_objects#Sending_notifications_from_a_task
 */
@Immutable
public abstract class ATaskNotificationListener implements NotificationListener {

    @Override
    public final void handleNotification(final Notification notification, final Object handback) {
        if (notification instanceof TaskExecutionNotification) {
            final TaskExecutionNotification cNotification = (TaskExecutionNotification) notification;
            if (cNotification.isUserNotification()) {
                handleNotification(cNotification);
            }
        }
    }

    protected abstract void handleNotification(TaskExecutionNotification notification);

}
