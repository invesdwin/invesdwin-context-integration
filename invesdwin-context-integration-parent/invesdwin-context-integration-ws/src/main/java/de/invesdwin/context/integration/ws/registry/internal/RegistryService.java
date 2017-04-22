package de.invesdwin.context.integration.ws.registry.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.Immutable;
import javax.inject.Named;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Organization;
import javax.xml.registry.infomodel.Service;
import javax.xml.registry.infomodel.ServiceBinding;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.springframework.core.io.ClassPathResource;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.integration.ws.IntegrationWsProperties;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.JaxrHelper;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;

@Immutable
@Named
public class RegistryService implements IRegistryService, IPreStartupHook {

    @Override
    public synchronized ServiceBinding registerServiceInstance(final String serviceName, final String accessURI)
            throws IOException {
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper();
            //load or create Organization
            Organization organization = helper.getOrganization(ORGANIZATION);
            if (organization == null) {
                organization = helper.addOrganization(ORGANIZATION);
            }
            //load or create Service
            Service service = helper.getService(organization, serviceName);
            if (service == null) {
                service = helper.addService(organization, serviceName);
            }
            //load or create Binding
            ServiceBinding binding = helper.getServiceBindung(service, accessURI);
            if (binding == null) {
                binding = helper.addServiceBinding(service, accessURI, new FDate());
            } else {
                binding = helper.refreshServiceBinding(binding, new FDate());
            }
            return binding;
        } catch (final JAXRException e) {
            throw new IOException(e);
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized void unregisterServiceInstance(final String serviceName, final URI accessUri)
            throws IOException {
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper();
            //Get current Services
            final Organization organization = helper.getOrganization(ORGANIZATION);
            Assertions.assertThat(organization)
                    .as("Organization [%s] to delete ServiceBinding [%s] was not found.", ORGANIZATION, accessUri)
                    .isNotNull();
            final Service service = helper.getService(organization, serviceName);
            Assertions.assertThat(service)
                    .as("Service [%s] to delete ServiceBinding [%s] was not found.", serviceName, accessUri)
                    .isNotNull();
            //Remove Binding
            final List<ServiceBinding> serviceBindings = new ArrayList<ServiceBinding>(service.getServiceBindings());
            for (final ServiceBinding binding : serviceBindings) {
                if (binding.getAccessURI().equalsIgnoreCase(accessUri.toString())) {
                    service.removeServiceBinding(binding);
                    helper.removeServiceBinding(binding);
                }
            }
            //maybe remove service
            if (service.getServiceBindings().size() == 0) {
                organization.removeService(service);
                helper.removeService(service);
            }
            //Eventually remove Organization
            /*
             * TODO: Organizations can't be deleted currently. See: https://issues.apache.org/jira/browse/SCOUT-108
             * 
             * also not needed since de.invesdwin organization is always there
             */
            //            if (organization.getServices().size() == 0) {
            //                helper.removeOrganization(organization);
            //            }
        } catch (final JAXRException e) {
            throw new IOException(e);
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ServiceBinding> queryServiceInstances(final String serviceName) throws IOException {
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper();
            final Organization organization = helper.getOrganization(ORGANIZATION);
            if (organization == null) {
                return null;
            }
            final Service service = helper.getService(organization, serviceName);
            if (service == null) {
                return null;
            } else {
                return service.getServiceBindings();
            }
        } catch (final JAXRException e) {
            throw new IOException(e);
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @Override
    public Boolean isServerReady() {
        try {
            return new JaxrHelper(false).isServerReady();
        } catch (final JAXRException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean isServerResponding() {
        try {
            return new JaxrHelper(false).isServerResponding();
        } catch (final JAXRException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void waitIfServerNotReady() {
        try {
            new JaxrHelper(false).waitIfServerNotReady();
        } catch (final JAXRException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void preStartup() throws Exception {
        //juddi xml does not support reading system properties anymore, so we have to do this workaround...
        final InputStream in = new ClassPathResource("META-INF/template.uddi.xml").getInputStream();
        String xmlStr = IOUtils.toString(in);
        xmlStr = xmlStr.replace("[REGISTRY_SERVER_URI]", IntegrationWsProperties.getRegistryServerUri().toString());
        final File file = getUddiConfigFile(true);
        FileUtils.writeStringToFile(file, xmlStr);
        in.close();
    }

    private File getUddiConfigFile(final boolean mkdir) throws IOException {
        final File folder = new File(ContextProperties.TEMP_CLASSPATH_DIRECTORY, "META-INF");
        if (mkdir) {
            FileUtils.forceMkdir(folder);
        }
        final File file = new File(folder, "uddi.xml");
        return file;
    }

}
