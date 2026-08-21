package de.invesdwin.context.integration.grid.jar.fork;

import java.io.File;
import java.net.URL;

import javax.annotation.concurrent.Immutable;

import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.string.Strings;

@Immutable
public final class CurrentJavaClasspathProvider implements IJavaClasspathProvider {

    public static final CurrentJavaClasspathProvider INSTANCE = new CurrentJavaClasspathProvider();

    private CurrentJavaClasspathProvider() {}

    @Override
    public String getClasspath() {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        String classpathStr = newClasspath(contextClassLoader);
        if (Strings.isNotBlank(classpathStr)) {
            return classpathStr;
        }
        final ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
        classpathStr = newClasspath(systemClassLoader);
        if (Strings.isNotBlank(classpathStr)) {
            return classpathStr;
        }
        //CHECKSTYLE:OFF
        return System.getProperty("java.class.path");
        //CHECKSTYLE:ON
    }

    protected String newClasspath(final ClassLoader classLoader) {
        final StringBuilder classpath = new StringBuilder();
        final URL[] urls = DynamicInstrumentationReflections.getURLs(classLoader);
        for (int i = 0; i < urls.length; i++) {
            final URL url = urls[i];
            if (i > 0) {
                classpath.append(File.pathSeparator);
            }
            classpath.append(url.toString());
        }
        final String classpathStr = classpath.toString();
        return classpathStr;
    }

}
