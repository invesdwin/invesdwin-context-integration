package de.invesdwin.context.integration.affinity;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.util.assertions.Assertions;
import net.openhft.affinity.AffinityLock;
import net.openhft.affinity.CpuLayout;
import net.openhft.affinity.impl.NoCpuLayout;

@ThreadSafe
public final class AffinityProperties {

    private static CpuLayout prevCpuLayout;
    private static boolean enabled = true;

    private AffinityProperties() {}

    public static synchronized void setEnabled(final boolean enabled) {
        final boolean prevEnabled = isEnabled();
        AffinityProperties.enabled = enabled;
        if (prevEnabled && !enabled) {
            Assertions.checkNull(prevCpuLayout);
            prevCpuLayout = AffinityLock.cpuLayout();
            AffinityLock.cpuLayout(new NoCpuLayout(0));
        } else if (!prevEnabled && enabled) {
            Assertions.checkNotNull(prevCpuLayout);
            AffinityLock.cpuLayout(prevCpuLayout);
            prevCpuLayout = null;
        }
    }

    public static synchronized boolean isEnabled() {
        return enabled;
    }

}
