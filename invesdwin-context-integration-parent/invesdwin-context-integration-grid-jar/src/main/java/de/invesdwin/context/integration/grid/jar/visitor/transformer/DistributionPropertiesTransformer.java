package de.invesdwin.context.integration.grid.jar.visitor.transformer;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.maven.plugins.shade.relocation.Relocator;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;

@NotThreadSafe
public class DistributionPropertiesTransformer implements ResourceTransformer {

    private static final String RESOURCE_PATH = "META-INF/env/distribution.properties";

    private final Map<String, String> distributionProperties;
    private final Properties mergedDistributionProperties = new Properties();
    private boolean fileEncountered = false;

    public DistributionPropertiesTransformer(final Map<String, String> distributionProperties) {
        this.distributionProperties = distributionProperties;
    }

    @Override
    public boolean canTransformResource(final String resource) {
        return RESOURCE_PATH.equals(resource);
    }

    @Override
    public void processResource(final String resource, final InputStream is, final List<Relocator> relocators)
            throws IOException {
        mergedDistributionProperties.load(is);
        fileEncountered = true;
    }

    @Override
    public boolean hasTransformedResource() {
        // Return true to ensure modifyOutputStream is called, even if the file wasn't originally on the classpath
        return fileEncountered || (distributionProperties != null && !distributionProperties.isEmpty());
    }

    @Override
    public void modifyOutputStream(final JarOutputStream os) throws IOException {
        if (distributionProperties != null) {
            mergedDistributionProperties.putAll(distributionProperties);
        }
        os.putNextEntry(new JarEntry(RESOURCE_PATH));
        mergedDistributionProperties.store(os, "Programmatically merged distribution properties");
    }
}