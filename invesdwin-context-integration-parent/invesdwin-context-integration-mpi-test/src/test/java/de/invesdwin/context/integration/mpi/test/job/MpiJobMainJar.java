package de.invesdwin.context.integration.mpi.test.job;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.annotation.concurrent.ThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;

@ThreadSafe
public final class MpiJobMainJar extends MergedClasspathJar {

    public static final MpiJobMainJar INSTANCE = new MpiJobMainJar();

    private MpiJobMainJar() {
        super(MergedClasspathJarFilter.MPI_YARN);
    }

    @Override
    protected JarOutputStream newJarOutputStream(final FileOutputStream fos) throws IOException {
        final Manifest manifest = new Manifest();
        final Attributes global = manifest.getMainAttributes();
        global.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        global.put(Attributes.Name.MAIN_CLASS, MpiJobMain.class.getName());
        return new JarOutputStream(fos, manifest);
    }

    @Override
    protected File newFile() {
        return new File(ContextProperties.getCacheDirectory(), MpiJobMain.class.getSimpleName() + ".jar");
    }

}
