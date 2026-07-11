package de.invesdwin.context.integration.jar.visitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.maven.plugins.shade.transformer.Log4j2PluginCacheFileTransformer;
import org.apache.maven.plugins.shade.resource.AppendingTransformer;
import org.apache.maven.plugins.shade.resource.ComponentsXmlResourceTransformer;
import org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.apache.maven.plugins.shade.resource.ServicesResourceTransformer;
import org.apache.maven.plugins.shade.resource.XmlAppendingTransformer;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.IClasspathResourceVisitor;
import de.invesdwin.maven.plugin.shade.RegexAppendingTransformer;
import de.invesdwin.maven.plugin.shade.WebFragmentTransformer;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@NotThreadSafe
public class MergedClasspathJarVisitor implements IClasspathResourceVisitor {

    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/spring.handlers</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/spring.schemas</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/spring.tooling</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/spring.factories</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/cxf/bus-extensions.txt</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/mime.types</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>META-INF/mailcap</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //    <resource>jasperreports_extension.properties</resource>
    //</transformer>
    private static final String[] MERGED_STRING_RESOURCES = { "META-INF/spring.handlers", "META-INF/spring.schemas",
            "META-INF/spring.tooling", "META-INF/spring.factories", "META-INF/cxf/bus-extensions.txt",
            "META-INF/mime.types", "META-INF/mailcap", "jasperreports_extension.properties" };
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.XmlAppendingTransformer">
    //    <resource>META-INF/aop.xml</resource>
    //</transformer>
    //<transformer
    //    implementation="org.apache.maven.plugins.shade.resource.XmlAppendingTransformer">
    //    <resource>META-INF/wsdl.plugin.xml</resource>
    //</transformer>
    private static final String[] MERGED_XML_RESOURCES = { "META-INF/aop.xml", "META-INF/wsdl.plugin.xml" };
    //CHECKSTYLE:OFF
    private static final Map<String, Object> MANIFEST_ENTRIES = new LinkedHashMap<String, Object>() {
        //CHECKSTYLE:ON
        {
            put("Multi-Release", "true");
        }
    };

    private final Set<String> duplicateResourcesFilter = ILockCollectionFactory.getInstance(false).newSet();

    private final JarOutputStream jarOut;
    private final IMergedClasspathJarFilter filter;
    private final List<ResourceTransformer> transformers;

    public MergedClasspathJarVisitor(final JarOutputStream jarOut, final IMergedClasspathJarFilter filter) {
        this.jarOut = jarOut;
        this.filter = filter;
        this.transformers = new ArrayList<ResourceTransformer>();
        for (final String mergedStringResource : MERGED_STRING_RESOURCES) {
            final AppendingTransformer transformer = new AppendingTransformer();
            Reflections.field("resource").ofType(String.class).in(transformer).set(mergedStringResource);
            transformers.add(transformer);
        }
        for (final String mergedXmlResource : MERGED_XML_RESOURCES) {
            final XmlAppendingTransformer transformer = new XmlAppendingTransformer();
            Reflections.field("resource").ofType(String.class).in(transformer).set(mergedXmlResource);
            transformers.add(transformer);
        }

        //<transformer
        //    implementation="de.invesdwin.maven.plugin.shade.RegexAppendingTransformer">
        //    <resource></resource>
        //</transformer>
        transformers.add(new RegexAppendingTransformer("ValidationMessages.*\\.properties"));
        //<transformer
        //    implementation="de.invesdwin.maven.plugin.shade.WebFragmentTransformer">
        //    <resource>META-INF/web-fragment.xml</resource>
        //</transformer>
        transformers.add(new WebFragmentTransformer("META-INF/web-fragment.xml"));
        //<transformer
        //    implementation="org.apache.maven.plugins.shade.resource.ComponentsXmlResourceTransformer" />
        transformers.add(new ComponentsXmlResourceTransformer());
        //<transformer
        //    implementation="org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer" />
        transformers.add(new PluginXmlResourceTransformer());
        //<transformer
        //    implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
        transformers.add(new ServicesResourceTransformer());
        //<transformer
        //    implementation="org.apache.logging.log4j.maven.plugins.shade.transformer.Log4j2PluginCacheFileTransformer" />
        transformers.add(new Log4j2PluginCacheFileTransformer());
        //<transformer
        //    implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
        //    <manifestEntries>
        //        <Multi-Release>true</Multi-Release>
        //    </manifestEntries>
        //</transformer>
        //        final ManifestResourceTransformer manifestResourceTransformer = new ManifestResourceTransformer();
        //        manifestResourceTransformer.setManifestEntries(MANIFEST_ENTRIES);
        //        transformers.add(manifestResourceTransformer);
    }

    @Override
    public boolean visit(final String fullPath, final String resourcePath, final InputStream inputStream) {
        boolean whitelisted = false;
        for (final String whitelistedResourcePattern : filter.getWhitelist()) {
            if (resourcePath.matches(whitelistedResourcePattern)) {
                whitelisted = true;
                break;
            }
        }
        if (!whitelisted) {
            for (final String blacklistedResourcePattern : filter.getBlacklist()) {
                if (resourcePath.matches(blacklistedResourcePattern)) {
                    return true;
                }
            }
        }

        try {
            for (final ResourceTransformer transformer : transformers) {
                if (transformer.canTransformResource(resourcePath)) {
                    transformer.processResource(resourcePath, inputStream, Collections.emptyList());
                    return true;
                }
            }

            if (duplicateResourcesFilter.add(resourcePath)) {
                final JarEntry entry = new JarEntry(resourcePath);
                jarOut.putNextEntry(entry);
                IOUtils.copy(inputStream, jarOut);
                jarOut.closeEntry();
            }
            return true;
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

    @Override
    public void finish() {
        try {
            for (final ResourceTransformer transformer : transformers) {
                if (transformer.hasTransformedResource()) {
                    transformer.modifyOutputStream(jarOut);
                    jarOut.closeEntry();
                }
            }
        } catch (final IOException e) {
            throw Err.process(e);
        }
    }

}
