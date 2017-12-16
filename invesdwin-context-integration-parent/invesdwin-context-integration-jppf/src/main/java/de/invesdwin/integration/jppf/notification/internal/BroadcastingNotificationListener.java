package de.invesdwin.integration.jppf.notification.internal;

import java.util.LinkedHashSet;
import java.util.Set;

import javax.annotation.concurrent.ThreadSafe;
import javax.management.Notification;
import javax.management.NotificationListener;

import de.invesdwin.util.collections.concurrent.AFastIterableDelegateSet;

@ThreadSafe
public class BroadcastingNotificationListener implements NotificationListener {

    private final AFastIterableDelegateSet<NotificationListener> listeners = new AFastIterableDelegateSet<NotificationListener>() {
        @Override
        protected Set<NotificationListener> newDelegate() {
            return new LinkedHashSet<NotificationListener>();
        }
    };

    public boolean registerNotificationListener(final NotificationListener l) {
        return listeners.add(l);
    }

    public boolean unregisterNotificationListener(final NotificationListener l) {
        return listeners.remove(l);
    }

    public void clear() {
        listeners.clear();
    }

    @Override
    public void handleNotification(final Notification notification, final Object handback) {
        final NotificationListener[] array = listeners.asArray(NotificationListener.class);
        for (int i = 0; i < array.length; i++) {
            array[i].handleNotification(notification, handback);
        }
    }

}
