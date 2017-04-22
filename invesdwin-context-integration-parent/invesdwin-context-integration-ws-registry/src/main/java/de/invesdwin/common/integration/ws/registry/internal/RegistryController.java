package de.invesdwin.common.integration.ws.registry.internal;

import java.io.IOException;
import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Organization;
import javax.xml.registry.infomodel.Service;
import javax.xml.registry.infomodel.ServiceBinding;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import de.invesdwin.common.integration.ws.registry.IRegistryService;
import de.invesdwin.common.integration.ws.registry.JaxrHelper;
import de.invesdwin.util.lang.Strings;

@Controller
@ThreadSafe
public class RegistryController {

    @Inject
    private IRegistryService registry;

    @RequestMapping("/ready")
    public void ready(final HttpServletResponse response) throws IOException {
        response.getOutputStream().print(RegistryHooker.isInitialized());
    }

    @RequestMapping("/query_{serviceName}")
    public void queryServiceInstances(final HttpServletResponse response,
            @PathVariable("serviceName") final String serviceName) throws IOException, JAXRException {
        final StringBuilder accessURIs = new StringBuilder();
        for (final ServiceBinding binding : registry.queryServiceInstances(serviceName)) {
            accessURIs.append(binding.getAccessURI());
            accessURIs.append("\n");
        }
        response.getOutputStream().print(Strings.chomp(accessURIs.toString()));
    }

    @SuppressWarnings("unchecked")
    @RequestMapping("/info")
    public void info(final HttpServletResponse response) throws JAXRException, IOException {
        JaxrHelper helper = null;
        try {
            final StringBuilder info = new StringBuilder();
            helper = new JaxrHelper();
            final Organization org = helper.getOrganization(IRegistryService.ORGANIZATION);
            final Collection<Service> services = org.getServices();
            info.append("There ");
            if (services.size() != 1) {
                info.append("are ");
            } else {
                info.append("is ");
            }
            info.append(services.size());
            info.append(" Service");
            if (services.size() != 1) {
                info.append("s");
            }
            info.append(":\n");
            int cService = 1;
            for (final Service s : services) {
                info.append(cService);
                info.append(". Service [");
                info.append(s.getName().getValue());
                info.append("] has ");
                final Collection<ServiceBinding> bindings = s.getServiceBindings();
                info.append(bindings.size());
                info.append(" ServiceBinding");
                if (bindings.size() != 1) {
                    info.append("s");
                }
                info.append(":\n");
                int cBinding = 1;
                for (final ServiceBinding b : bindings) {
                    final String heartbeat = b.getDescription().getValue();
                    info.append(cService);
                    info.append(".");
                    info.append(cBinding++);
                    info.append(". ServiceBinding [");
                    info.append(b.getAccessURI());
                    info.append("] exists since [");
                    info.append(heartbeat);
                    info.append("]\n");
                }
                cService++;
            }
            response.getOutputStream().print(Strings.chomp(info.toString()));
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @RequestMapping("clientip")
    public void clientip(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        response.getOutputStream().print(request.getRemoteAddr());
    }

}
