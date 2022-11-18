package de.invesdwin.context.integration.ws.registry.internal;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.concurrent.ThreadSafe;
import jakarta.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.springframework.stereotype.Controller;
import org.springframework.util.Base64Utils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.context.integration.marshaller.MarshallerJsonJackson;
import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.IRestRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingDao;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingEntity;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;
import de.invesdwin.util.lang.uri.connect.IURIsConnect;
import de.invesdwin.util.lang.uri.connect.InputStreamHttpResponse;
import de.invesdwin.util.lang.uri.header.Headers;
import de.invesdwin.util.math.Integers;
import de.invesdwin.util.math.Longs;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.duration.Duration;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Controller
@ThreadSafe
@RequestMapping(IRestRegistryService.REGISTRY)
public class RegistryController implements IRestRegistryService, IStartupHook {

    private volatile boolean available = false;

    @Inject
    private IRegistryService registry;
    @Inject
    private ServiceBindingDao serviceBindingDao;

    @RequestMapping(QUERY_SERVICE_BINDINGS)
    public void queryServiceBindings(final HttpServletResponse response,
            @PathVariable(SERVICE_NAME) final String serviceName) throws IOException {
        final String serviceNameDecoded = URIs.decode(serviceName);
        final Collection<ServiceBinding> instances = registry.queryServiceBindings(serviceNameDecoded);
        response.getOutputStream().print(MarshallerJsonJackson.toJson(instances));
    }

    @RequestMapping(REGISTER_SERVICE_BINDING)
    public void registerServiceBinding(final HttpServletResponse response,
            @PathVariable(SERVICE_NAME) final String serviceName, @PathVariable(ACCESS_URI) final String accessUri)
            throws IOException {
        final String serviceNameDecoded = URIs.decode(serviceName);
        final URI accessUriDecoded = URIs.asUri(new String(Base64Utils.decode(accessUri.getBytes())));
        final ServiceBinding instance = registry.registerServiceBinding(serviceNameDecoded, accessUriDecoded);
        response.getOutputStream().print(MarshallerJsonJackson.toJson(instance));
    }

    @RequestMapping(UNREGISTER_SERVICE_BINDING)
    public void unregisterServiceBinding(final HttpServletResponse response,
            @PathVariable(SERVICE_NAME) final String serviceName, @PathVariable(ACCESS_URI) final String accessUri)
            throws IOException {
        final String serviceNameDecoded = URIs.decode(serviceName);
        final URI accessUriDecoded = URIs.asUri(new String(Base64Utils.decode(accessUri.getBytes())));
        final ServiceBinding instance = registry.unregisterServiceBinding(serviceNameDecoded, accessUriDecoded);
        response.getOutputStream().print(MarshallerJsonJackson.toJson(instance));
    }

    @RequestMapping(INFO)
    public void info(final HttpServletResponse response) throws IOException {
        final StringBuilder info = new StringBuilder();
        final List<ServiceBindingEntity> serviceBindings = serviceBindingDao.findAll();
        final ALoadingCache<String, List<ServiceBindingEntity>> groupedServiceBindings = new ALoadingCache<String, List<ServiceBindingEntity>>() {
            @Override
            protected List<ServiceBindingEntity> loadValue(final String key) {
                return new ArrayList<>();
            }
        };
        for (final ServiceBindingEntity s : serviceBindings) {
            groupedServiceBindings.get(s.getName()).add(s);
        }
        info.append("There ");
        if (groupedServiceBindings.size() != 1) {
            info.append("are ");
        } else {
            info.append("is ");
        }
        info.append(groupedServiceBindings.size());
        info.append(" Service");
        if (groupedServiceBindings.size() != 1) {
            info.append("s");
        }
        info.append(":\n");
        int cService = 1;
        for (final Entry<String, List<ServiceBindingEntity>> s : groupedServiceBindings.entrySet()) {
            info.append(cService);
            info.append(". Service [");
            info.append(s.getKey());
            info.append("] has ");
            final Collection<ServiceBindingEntity> bindings = s.getValue();
            info.append(bindings.size());
            info.append(" ServiceBinding");
            if (bindings.size() != 1) {
                info.append("s");
            }
            info.append(":\n");
            int cBinding = 1;
            for (final ServiceBindingEntity b : bindings) {
                final FDate created = FDate.valueOf(b.getCreated());
                final FDate updated = FDate.valueOf(b.getUpdated());
                info.append(cService);
                info.append(".");
                info.append(cBinding++);
                info.append(". ServiceBinding [");
                info.append(b.getAccessUri());
                info.append("] exists since [");
                info.append(created.toString());
                info.append(" ");
                info.append(new Duration(created).toString(FTimeUnit.MILLISECONDS));
                info.append("] with last heartbeat [");
                info.append(updated.toString());
                info.append(" ");
                info.append(new Duration(updated).toString(FTimeUnit.MILLISECONDS));
                info.append("]\n");
            }
            cService++;
        }
        response.getOutputStream().print(Strings.chomp(info.toString()));
    }

    @RequestMapping(CLIENTIP)
    public void clientip(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        response.getOutputStream().print(request.getRemoteAddr());
    }

    @RequestMapping(AVAILABLE)
    public void available(final HttpServletResponse response) throws IOException {
        response.getOutputStream().print(available);
    }

    @RequestMapping(value = GATEWAY, method = { RequestMethod.GET, RequestMethod.HEAD, RequestMethod.DELETE,
            RequestMethod.OPTIONS, RequestMethod.PATCH, RequestMethod.POST, RequestMethod.PUT, RequestMethod.TRACE })
    public void gateway(final HttpServletRequest request, final HttpServletResponse response) {
        final String gatewayRequestStr = request.getHeader(GATEWAY_REQUEST);
        final String gatewayTimeoutStr = request.getHeader(GATEWAY_TIMEOUT);
        final String gatewayHeadersStr = request.getHeader(GATEWAY_HEADERS);

        final IURIsConnect connect = URIs.connect(gatewayRequestStr);
        final Integer durationMs = Integers.valueOfOrNull(gatewayTimeoutStr);
        if (durationMs != null) {
            connect.setNetworkTimeout(new Duration(durationMs, FTimeUnit.MILLISECONDS));
        }
        connect.setMethod(request.getMethod());
        connect.setContentType(request.getContentType());
        try {
            connect.setBody(request.getInputStream());
        } catch (final IOException e1) {
            throw new RuntimeException(e1);
        }

        final Map<String, String> headers = Headers.decode(gatewayHeadersStr);
        for (final Entry<String, String> entry : headers.entrySet()) {
            connect.putHeader(entry.getKey(), entry.getValue());
        }

        try (InputStreamHttpResponse input = connect.downloadInputStream()) {
            final ServletOutputStream output = response.getOutputStream();
            IOUtils.copy(input, output);
            final Long contentLength = Longs.valueOfOrNull(input.getResponse().getHeader(Headers.CONTENT_LENGTH));
            if (contentLength != null) {
                response.setContentLengthLong(contentLength);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void startup() throws Exception {
        available = true;
    }

}
