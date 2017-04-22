package de.invesdwin.common.integration.ws.registry.internal;

import java.io.File;
import java.io.InputStream;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;
import javax.xml.registry.JAXRException;
import javax.xml.registry.infomodel.Organization;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.juddi.Registry;
import org.springframework.core.io.ClassPathResource;

import de.invesdwin.common.integration.ws.IntegrationWsProperties;
import de.invesdwin.common.integration.ws.registry.IRegistryService;
import de.invesdwin.common.integration.ws.registry.JaxrHelper;
import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.beans.hook.IPreStartupHook;
import de.invesdwin.context.beans.hook.IStartupHook;
import de.invesdwin.util.shutdown.IShutdownHook;

@ThreadSafe
@Named
public class RegistryHooker implements IStartupHook, IPreStartupHook, IShutdownHook {

    private static volatile boolean initialized;

    @Override
    public void startup() throws Exception {
        Registry.start();
        createOrganization();
        setInitialized();
    }

    private static void setInitialized() {
        initialized = true;
    }

    public static boolean isInitialized() {
        return initialized;
    }

    /**
     * Organization is already created so that clients don't get into a race condition on startup to create it. This
     * would lead to duplicate Organizations which are not allowed.
     */
    private void createOrganization() throws JAXRException {
        JaxrHelper helper = null;
        try {
            helper = new JaxrHelper(false);
            final Organization organization = helper.getOrganization(IRegistryService.ORGANIZATION);
            if (organization == null) {
                helper.addOrganization(IRegistryService.ORGANIZATION);
            }
        } finally {
            if (helper != null) {
                helper.close();
            }
        }
    }

    @Override
    public void shutdown() throws Exception {
        Registry.stop();
    }

    @Override
    public void preStartup() throws Exception {
        //juddi xml does not support reading system properties anymore, so we have to do this workaround...
        final InputStream in = new ClassPathResource("META-INF/template.juddiv3.xml").getInputStream();
        String xmlStr = IOUtils.toString(in);
        xmlStr = xmlStr.replace("[REGISTRY_SERVER_URI]", IntegrationWsProperties.getRegistryServerUri().toString());
        FileUtils.writeStringToFile(new File(ContextProperties.TEMP_CLASSPATH_DIRECTORY, "juddiv3.xml"), xmlStr);
        in.close();
    }

}
