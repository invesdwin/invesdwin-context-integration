package de.invesdwin.context.integration.mpi.test.job;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes.Name;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.annotation.concurrent.ThreadSafe;

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
        manifest.getMainAttributes().put(Name.MAIN_CLASS, MpiJobMain.class.getName());
        return new JarOutputStream(fos, manifest);
    }

}
