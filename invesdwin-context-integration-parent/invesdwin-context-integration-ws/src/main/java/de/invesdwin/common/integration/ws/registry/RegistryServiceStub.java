package de.invesdwin.common.integration.ws.registry;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.LocalizedString;
import javax.xml.registry.infomodel.ServiceBinding;

import org.apache.ws.scout.registry.infomodel.InternationalStringImpl;
import org.apache.ws.scout.registry.infomodel.ServiceBindingImpl;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.context.test.stub.StubSupport;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.concurrent.AFastIterableDelegateSet;

@Named
@ThreadSafe
public class RegistryServiceStub extends StubSupport implements IRegistryService {

    private static boolean enabled = true;
    private static final Map<String, String> SERVICENAME_ACCESSURI_OVERRIDES = new ConcurrentHashMap<String, String>();
    private final Set<ServiceBinding> registeredBindings = Collections
            .synchronizedSet(new AFastIterableDelegateSet<ServiceBinding>() {
                @Override
                protected Set<ServiceBinding> newDelegate() {
                    return new LinkedHashSet<ServiceBinding>();
                }
            });

    @Override
    public void setUpContext(final ATest test, final TestContext ctx) {
        if (enabled) {
            ctx.replace(IRegistryService.class, this.getClass());
        } else {
            ctx.deactivate(this.getClass());
        }
    }

    @Override
    public void tearDownOnce(final ATest test) throws Exception {
        setEnabled(true);
    }

    public static void setEnabled(final boolean enabled) {
        RegistryServiceStub.enabled = enabled;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    public static void override(final String serviceName, final String accessURI) {
        SERVICENAME_ACCESSURI_OVERRIDES.put(serviceName, accessURI);
    }

    @Override
    public ServiceBinding registerServiceInstance(final String serviceName, final String accessURI) {
        try {
            final ServiceBinding binding = new ServiceBindingImpl(null);
            binding.setName(new InternationalStringImpl(Locale.getDefault(), serviceName,
                    LocalizedString.DEFAULT_CHARSET_NAME));
            final String overrideURI = SERVICENAME_ACCESSURI_OVERRIDES.get(serviceName);
            if (overrideURI != null) {
                binding.setAccessURI(overrideURI);
            } else {
                binding.setAccessURI(accessURI);
            }
            Assertions.assertThat(registeredBindings.add(binding)).isTrue();
            return binding;
        } catch (final JAXRException e) {
            throw Err.process(e);
        }
    }

    @Override
    public void unregisterServiceInstance(final String serviceName, final URI accessUri) {
        try {
            for (final ServiceBinding binding : registeredBindings) {
                if (binding.getAccessURI() != null && binding.getAccessURI().equalsIgnoreCase(accessUri.toString())) {
                    Assertions.assertThat(registeredBindings.remove(binding)).isTrue();
                }
            }
        } catch (final JAXRException e) {
            throw Err.process(e);
        }
    }

    @Override
    public Collection<ServiceBinding> queryServiceInstances(final String serviceName) {
        try {
            final List<ServiceBinding> found = new ArrayList<ServiceBinding>();
            for (final ServiceBinding binding : registeredBindings) {
                if (binding.getName().getValue().equals(serviceName)) {
                    found.add(binding);
                }
            }
            if (found.size() == 0) {
                final String overrideURI = SERVICENAME_ACCESSURI_OVERRIDES.get(serviceName);
                found.add(registerServiceInstance(serviceName, overrideURI));
            }
            return found;
        } catch (final JAXRException e) {
            throw Err.process(e);
        }
    }

    @Override
    public Boolean isServerReady() {
        return true;
    }

    @Override
    public boolean isServerResponding() {
        return true;
    }

    @Override
    public void waitIfServerNotReady() {}

}
