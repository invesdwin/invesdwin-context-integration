package de.invesdwin.context.integration.ws.registry.internal;

import java.io.IOException;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingDao;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingEntity;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import jakarta.inject.Inject;
import jakarta.inject.Named;

@ThreadSafe
@Named
public class ServiceBindingHeartbeatChecker implements IStartupHook {

    private final Log log = new Log(this);
    private volatile boolean started;
    @Inject
    private ServiceBindingDao serviceBindingDao;

    @Override
    public void startup() throws Exception {
        started = true;
    }

    @Scheduled(fixedDelay = IntegrationWsProperties.SERVICE_BINDING_HEARTBEAT_REFRESH_INVERVAL_MILLIS)
    public void purgeOldBindings() throws IOException {
        if (!started) {
            return;
        }
        final Collection<ServiceBindingEntity> bindings = serviceBindingDao.findAll();
        for (final ServiceBindingEntity b : bindings) {
            final FDate heartbeat = FDate.valueOf(b.getUpdated());
            if (heartbeat == null) {
                log.warn("Purging ServiceBinding [%s] for Service [%s] because it has no heartbeat.", b.getAccessUri(),
                        b.getName());
                serviceBindingDao.delete(b);
            } else {
                final long heartbeatTimestamp = heartbeat.millisValue();
                final boolean shouldBeDiscarded = System.currentTimeMillis()
                        - heartbeatTimestamp > IntegrationWsProperties.SERVICE_BINDING_HEARTBEAT_PURGE_INTERVAL_MILLIS;
                if (shouldBeDiscarded) {
                    log.warn(
                            "Purging ServiceBinding [%s] for Service [%s] because the last heartbeat [%s %s] is too long ago.",
                            b.getAccessUri(), b.getName(), heartbeat,
                            new Duration(heartbeat).toString(FTimeUnit.MILLISECONDS));
                    serviceBindingDao.delete(b);
                }
            }
        }
    }

}
