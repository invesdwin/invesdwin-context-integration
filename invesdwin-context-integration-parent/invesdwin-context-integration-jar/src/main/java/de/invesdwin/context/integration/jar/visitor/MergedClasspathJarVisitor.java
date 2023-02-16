package de.invesdwin.context.integration.jar.visitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.commons.io.IOUtils;
import org.apache.maven.plugins.shade.resource.AppendingTransformer;
import org.apache.maven.plugins.shade.resource.ComponentsXmlResourceTransformer;
import org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer;
import org.apache.maven.plugins.shade.resource.ResourceTransformer;
import org.apache.maven.plugins.shade.resource.ServicesResourceTransformer;
import org.apache.maven.plugins.shade.resource.XmlAppendingTransformer;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.IClasspathResourceVisitor;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.reflection.Reflections;

@NotThreadSafe
public class MergedClasspathJarVisitor implements IClasspathResourceVisitor {

    //    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //        <resource>META-INF/spring.handlers</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //        <resource>META-INF/spring.schemas</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //        <resource>META-INF/spring.tooling</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //        <resource>META-INF/spring.factories</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.AppendingTransformer">
    //        <resource>META-INF/cxf/bus-extensions.txt</resource>
    //    </transformer>
    private static final String[] MERGED_STRING_RESOURCES = { "META-INF/spring.handlers", "META-INF/spring.schemas",
            "META-INF/spring.tooling", "META-INF/spring.factories", "META-INF/cxf/bus-extensions.txt" };
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.XmlAppendingTransformer">
    //        <resource>META-INF/web-fragment.xml</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.XmlAppendingTransformer">
    //        <resource>META-INF/aop.xml</resource>
    //    </transformer>
    //    <transformer implementation="org.apache.maven.plugins.shade.resource.XmlAppendingTransformer">
    //        <resource>META-INF/wsdl.plugin.xml</resource>
    //    </transformer>
    private static final String[] MERGED_XML_RESOURCES = { "META-INF/web-fragment.xml", "META-INF/aop.xml",
            "META-INF/wsdl.plugin.xml" };

    private final Set<String> duplicateResourcesFilter = new HashSet<String>();

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
        //    <transformer implementation="org.apache.maven.plugins.shade.resource.ComponentsXmlResourceTransformer" />
        transformers.add(new ComponentsXmlResourceTransformer());
        //    <transformer implementation="org.apache.maven.plugins.shade.resource.PluginXmlResourceTransformer" />
        transformers.add(new PluginXmlResourceTransformer());
        //    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer" />
        transformers.add(new ServicesResourceTransformer());
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
