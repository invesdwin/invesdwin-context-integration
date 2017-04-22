package de.invesdwin.context.integration.ws.registry.internal;

import java.io.IOException;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Organization;
import javax.xml.registry.infomodel.Service;
import javax.xml.registry.infomodel.ServiceBinding;

import org.springframework.scheduling.annotation.Scheduled;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.JaxrHelper;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
@Named
public class ServiceBindingHeartbeatChecker implements IStartupHook {

    private final Log log = new Log(this);
    private volatile boolean started;

    @Override
    public void startup() throws Exception {
        started = true;
    }

    @SuppressWarnings("unchecked")
    @Scheduled(fixedDelay = IntegrationWsProperties.SERVICE_BINDING_HEARTBEAT_PURGE_INTERVAL_MILLIS)
    public void purgeOldBindings() throws IOException {
        if (!started) {
            return;
        }
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper();
            final Organization org = helper.getOrganization(IRegistryService.ORGANIZATION);
            if (org == null) {
                return;
            }
            final Collection<Service> services = org.getServices();
            for (final Service s : services) {
                final Collection<ServiceBinding> bindings = s.getServiceBindings();
                for (final ServiceBinding b : bindings) {
                    final FDate heartbeat = getDescription(b);
                    if (heartbeat == null) {
                        log.warn("Purging ServiceBinding [%s] for Service [%s] because it has no heartbeat.",
                                b.getAccessURI(), s.getName().getValue());
                        helper.removeServiceBinding(b);
                    } else {
                        final long heartbeatTimestamp = heartbeat.millisValue();
                        final boolean shouldBeDiscarded = System.currentTimeMillis()
                                - heartbeatTimestamp > IntegrationWsProperties.SERVICE_BINDING_HEARTBEAT_PURGE_INTERVAL_MILLIS;
                        if (shouldBeDiscarded) {
                            log.warn(
                                    "Purging ServiceBinding [%s] for Service [%s] because the last heartbeat [%s] is too long ago.",
                                    b.getAccessURI(), s.getName().getValue(), heartbeat);
                            helper.removeServiceBinding(b);
                        }
                    }
                }
            }
        } catch (final JAXRException e) {
            throw new IOException(e);
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    private FDate getDescription(final ServiceBinding b) throws JAXRException {
        if (b.getDescription() == null || Strings.isBlank(b.getDescription().getValue())) {
            return null;
        }
        try {
            return FDate.valueOf(b.getDescription().getValue(), JaxrHelper.DESCRIPTION_HEARTBEAT_FORMAT);
        } catch (final Throwable t) { //SUPPRESS CHECKSTYLE illegal catch
            Err.process(new Exception("ServiceBinding [" + b.getAccessURI()
                    + "] heartbeat could not be checked because an error occured during that.", t));
            return null;
        }
    }

}
