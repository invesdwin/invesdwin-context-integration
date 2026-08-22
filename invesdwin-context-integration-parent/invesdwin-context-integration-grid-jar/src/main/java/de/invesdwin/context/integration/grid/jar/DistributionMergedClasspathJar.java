package de.invesdwin.context.integration.grid.jar;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.grid.jar.visitor.MergedClasspathJarVisitor;
import de.invesdwin.context.integration.grid.jar.visitor.filter.CombinedMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.jar.visitor.filter.DistributionMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.jar.visitor.filter.IMergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.jar.visitor.transformer.DistributionPropertiesTransformer;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.IClasspathResourceVisitor;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;

@ThreadSafe
public class DistributionMergedClasspathJar extends MergedClasspathJar {

    public DistributionMergedClasspathJar() {
        super();
    }

    public DistributionMergedClasspathJar(final IMergedClasspathJarFilter filter) {
        super(filter);
    }

    public DistributionMergedClasspathJar(final IMergedClasspathJarFilter filter, final Class<?> mainClass) {
        super(filter, mainClass);
    }

    @Override
    protected IClasspathResourceVisitor newMergedClasspathJarVisitor(final JarOutputStream jarOut,
            final IMergedClasspathJarFilter filter) {
        final IMergedClasspathJarFilter combinedFilter = CombinedMergedClasspathJarFilter.ofNullable(filter,
                DistributionMergedClasspathJarFilter.DISTRIBUTION);
        final MergedClasspathJarVisitor visitor = new MergedClasspathJarVisitor(jarOut, combinedFilter);
        final Map<String, String> distributionProperties = newDistributionProperties();
        if (distributionProperties != null && !distributionProperties.isEmpty()) {
            visitor.addTransformer(new DistributionPropertiesTransformer(distributionProperties));
        }
        return visitor;
    }

    protected Map<String, String> newDistributionProperties() {
        final Map<String, String> properties = ILockCollectionFactory.getInstance(false).newLinkedMap();
        final File systemPropertiesFile = new File(ContextProperties.getHomeDirectory(), "system.properties");

        if (systemPropertiesFile.exists() && systemPropertiesFile.isFile()) {
            try (InputStream is = new FileInputStream(systemPropertiesFile)) {
                final Properties props = new Properties();
                props.load(is);
                for (final String key : props.stringPropertyNames()) {
                    if (key.startsWith("de.invesdwin.context.integration.")) {
                        properties.put(key, props.getProperty(key));
                    }
                }
            } catch (final java.io.IOException e) {
                throw Err.process(e);
            }
        }
        return properties;
    }

}
