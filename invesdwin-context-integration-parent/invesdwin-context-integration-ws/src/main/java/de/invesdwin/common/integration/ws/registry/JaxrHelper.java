package de.invesdwin.common.integration.ws.registry;

import java.util.Arrays;
import java.util.Collection;

import javax.annotation.concurrent.NotThreadSafe;
import javax.xml.registry.BulkResponse;
import javax.xml.registry.FindQualifier;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Key;
import javax.xml.registry.infomodel.Organization;
import javax.xml.registry.infomodel.Service;
import javax.xml.registry.infomodel.ServiceBinding;

import org.springframework.beans.factory.annotation.Configurable;

import de.invesdwin.common.integration.ws.registry.internal.AJaxrHelper;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;

@NotThreadSafe
@Configurable
public class JaxrHelper extends AJaxrHelper {

    public static final String DESCRIPTION_HEARTBEAT_FORMAT = FDate.FORMAT_ISO_DATE_TIME_MS;

    public JaxrHelper(final boolean waitIfServerNotReady) throws JAXRException {
        super(waitIfServerNotReady);
    }

    public JaxrHelper() throws JAXRException {
        super();
    }

    public Organization getOrganization(final String organizationName) throws JAXRException {
        final BulkResponse response = bqm.findOrganizations(Arrays.asList(FindQualifier.EXACT_NAME_MATCH),
                Arrays.asList(organizationName), null, null, null, null);
        Assertions.assertThat(response.getCollection().size())
                .as("Organization [%s] exists more than once.", organizationName)
                .isLessThanOrEqualTo(1);
        final Organization organization = getSingleResponse(response);
        return organization;
    }

    public Organization addOrganization(final String organizationName) throws JAXRException {
        final Organization organization = blm.createOrganization(organizationName);
        final BulkResponse response = blm.saveOrganizations(Arrays.asList(organization));
        final Key key = getSingleResponse(response);
        organization.setKey(key);
        return organization;
    }

    public void removeOrganization(final Organization organization) throws JAXRException {
        final BulkResponse response = blm.deleteOrganizations(Arrays.asList(organization.getKey()));
        final Key key = getSingleResponse(response);
        if (key != null) {
            Assertions.assertThat(key).isEqualTo(organization.getKey());
        }
    }

    public Service getService(final Organization organization, final String serviceName) throws JAXRException {
        /*
         * Getting the Organization directly out of the Service doesn't work because the content of the
         * InternationalString is null interestingly...
         */
        final BulkResponse response = bqm.findServices(organization.getKey(),
                Arrays.asList(FindQualifier.EXACT_NAME_MATCH), Arrays.asList(serviceName), null, null);
        final Collection<Service> services = getResponse(response);
        if (services.size() > 1) {
            return mergeServices(services);
        } else if (services.size() == 1) {
            return services.iterator().next();
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Service mergeServices(final Collection<Service> services) throws JAXRException {
        Service firstService = null;
        for (final Service service : services) {
            if (firstService == null) {
                firstService = service;
            } else {
                final Collection<ServiceBinding> serviceBindings = service.getServiceBindings();
                for (final ServiceBinding binding : serviceBindings) {
                    final FDate heartbeat = FDate.valueOf(binding.getDescription().getValue(),
                            DESCRIPTION_HEARTBEAT_FORMAT);
                    addServiceBinding(firstService, binding.getAccessURI(), heartbeat);
                }
                service.removeServiceBindings(serviceBindings);
                removeService(service);
            }
        }
        return firstService;
    }

    public Service addService(final Organization organization, final String serviceName) throws JAXRException {
        final Service service = blm.createService(serviceName);
        organization.addService(service);
        final BulkResponse resonse = blm.saveServices(Arrays.asList(service));
        final Key key = getSingleResponse(resonse);
        service.setKey(key);
        return service;
    }

    public void removeService(final Service service) throws JAXRException {
        final BulkResponse response = blm.deleteServices(Arrays.asList(service.getKey()));
        final Key key = getSingleResponse(response);
        if (key != null) {
            Assertions.assertThat(key).isEqualTo(service.getKey());
        }
    }

    public ServiceBinding getServiceBindung(final Service service, final String accessURI) throws JAXRException {
        @SuppressWarnings("unchecked")
        final Collection<ServiceBinding> bindings = service.getServiceBindings();
        ServiceBinding binding = null;
        for (final ServiceBinding b : bindings) {
            if (b.getAccessURI().equals(accessURI)) {
                Assertions.assertThat(binding)
                        .as("ServiceBinding [%s] exists more than once Service [%s]", accessURI,
                                service.getName().getValue())
                        .isNull();
                binding = b;
            }
        }
        return binding;
    }

    public ServiceBinding addServiceBinding(final Service service, final String accessURI, final FDate heartbeat)
            throws JAXRException {
        final ServiceBinding binding = blm.createServiceBinding();
        binding.setAccessURI(accessURI);
        binding.setName(service.getName());
        updateServiceBindingDescription(binding, heartbeat);

        service.addServiceBinding(binding);
        final BulkResponse response = blm.saveServiceBindings(Arrays.asList(binding));
        final Key key = getSingleResponse(response);
        binding.setKey(key);
        return binding;
    }

    public ServiceBinding refreshServiceBinding(final ServiceBinding binding, final FDate heartbeat)
            throws JAXRException {
        updateServiceBindingDescription(binding, heartbeat);
        final BulkResponse response = blm.saveServiceBindings(Arrays.asList(binding));
        final Key key = getSingleResponse(response);
        binding.setKey(key);
        return binding;
    }

    public void removeServiceBinding(final ServiceBinding binding) throws JAXRException {
        final BulkResponse response = blm.deleteServiceBindings(Arrays.asList(binding.getKey()));
        final Key key = getSingleResponse(response);
        if (key != null) {
            Assertions.assertThat(key).isEqualTo(binding.getKey());
        }
    }

    private void updateServiceBindingDescription(final ServiceBinding binding, final FDate heartbeat)
            throws JAXRException {
        binding.setDescription(blm.createInternationalString(heartbeat.toString(DESCRIPTION_HEARTBEAT_FORMAT)));
    }

}
