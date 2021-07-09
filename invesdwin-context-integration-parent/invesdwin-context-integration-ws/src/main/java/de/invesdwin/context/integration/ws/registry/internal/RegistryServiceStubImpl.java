package de.invesdwin.context.integration.ws.registry.internal;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.fast.concurrent.ASynchronizedFastIterableDelegateSet;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
public class RegistryServiceStubImpl implements IRegistryService {

    private static boolean enabled = true;
    private static final Map<String, URI> SERVICENAME_ACCESSURI_OVERRIDES = new ConcurrentHashMap<String, URI>();
    private final ASynchronizedFastIterableDelegateSet<ServiceBinding> registeredBindings = new ASynchronizedFastIterableDelegateSet<ServiceBinding>() {
        @Override
        protected Set<ServiceBinding> newDelegate() {
            return new LinkedHashSet<ServiceBinding>();
        }
    };

    public static void setEnabled(final boolean enabled) {
        RegistryServiceStubImpl.enabled = enabled;
    }

    public static boolean isEnabled() {
        return enabled;
    }

    public static void override(final String serviceName, final URI accessURI) {
        SERVICENAME_ACCESSURI_OVERRIDES.put(serviceName, accessURI);
    }

    @Override
    public ServiceBinding registerServiceBinding(final String serviceName, final URI accessURI) {
        final ServiceBinding binding = new ServiceBinding();
        binding.setName(serviceName);
        final URI overrideURI = SERVICENAME_ACCESSURI_OVERRIDES.get(serviceName);
        if (overrideURI != null) {
            binding.setAccessUri(overrideURI);
        } else {
            binding.setAccessUri(accessURI);
        }
        Assertions.assertThat(registeredBindings.add(binding)).isTrue();
        return binding;
    }

    @Override
    public ServiceBinding unregisterServiceBinding(final String serviceName, final URI accessUri) {
        for (final ServiceBinding binding : registeredBindings) {
            if (binding.getName().equals(serviceName)
                    && binding.getAccessUri().toString().equals(accessUri.toString())) {
                Assertions.assertThat(registeredBindings.remove(binding)).isTrue();
                binding.setDeleted(new FDate().jodaTimeValue().toDateTime());
                return binding;
            }
        }
        return null;
    }

    @Override
    public Collection<ServiceBinding> queryServiceBindings(final String serviceName) {
        final List<ServiceBinding> found = new ArrayList<ServiceBinding>();
        for (final ServiceBinding binding : registeredBindings) {
            if (binding.getName().equals(serviceName)) {
                found.add(binding);
            }
        }
        if (found.size() == 0) {
            final URI overrideURI = SERVICENAME_ACCESSURI_OVERRIDES.get(serviceName);
            if (overrideURI != null) {
                found.add(registerServiceBinding(serviceName, overrideURI));
            }
        }
        return found;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

}
