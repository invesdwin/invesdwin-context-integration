package de.invesdwin.context.integration.grid.jar.fork;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.StartedProcess;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.log.error.Err;

/**
 * Use this job helper to upgrade from an older java version to a newer one. This might be needed on hadoop clusters
 * that run on older JVMs.
 */
@NotThreadSafe
public final class ForkProcessHelper {

    private IJavaHomeProvider javaHomeProvider = CurrentJavaHomeProvider.INSTANCE;
    private IJavaClasspathProvider javaClasspathProvider = CurrentJavaClasspathProvider.INSTANCE;

    public ForkProcessHelper() {}

    public ForkProcessHelper setJavaHomeProvider(final IJavaHomeProvider javaHomeProvider) {
        this.javaHomeProvider = javaHomeProvider;
        return this;
    }

    public IJavaHomeProvider getJavaHomeProvider() {
        return javaHomeProvider;
    }

    public void setJavaClasspathProvider(final IJavaClasspathProvider javaClasspathProvider) {
        this.javaClasspathProvider = javaClasspathProvider;
    }

    public IJavaClasspathProvider getJavaClasspathProvider() {
        return javaClasspathProvider;
    }

    public void fork(final Class<?> mainClass, final String[] args) {
        final String classpath = javaClasspathProvider.getClasspath();
        fork(classpath, mainClass, args);
    }

    public void fork(final File jarFile, final Class<?> mainClass, final String[] args) {
        fork(jarFile.getAbsolutePath(), mainClass, args);
    }

    public void fork(final String classpath, final Class<?> mainClass, final String[] args) {
        fork(classpath, mainClass.getName(), args);
    }

    public void fork(final String classpath, final String mainClassName, final String[] args) {
        try {
            final File javaHome = javaHomeProvider.getJavaHome();
            final List<String> commands = new ArrayList<>();
            commands.add(new File(javaHome, "bin/java").getAbsolutePath());
            commands.add("-classpath");
            commands.add(classpath);
            commands.add(mainClassName);
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    commands.add(args[i]);
                }
            }
            final Slf4jStream stream = Slf4jStream.of(ForkProcessHelper.class);
            new ProcessExecutor().command(commands)
                    .destroyOnExit()
                    .exitValueNormal()
                    .redirectOutput(stream.asInfo())
                    .redirectError(stream.asWarn())
                    .environment(System.getenv())
                    .stopper(new ProcessStopper() {
                        @Override
                        public void stop(final Process process) {
                            process.destroy();
                        }
                    })
                    .execute();
        } catch (final Throwable e) {
            throw Err.process(e);
        }
    }

    public StartedProcess forkAsync(final Class<?> mainClass, final String[] args) {
        final String classpath = javaClasspathProvider.getClasspath();
        return forkAsync(classpath, mainClass.getName(), args);
    }

    public StartedProcess forkAsync(final String classpath, final String mainClassName, final String[] args) {
        try {
            final File javaCommand = javaHomeProvider.getJavaCommand();
            final List<String> commands = new ArrayList<>();
            commands.add(javaCommand.getAbsolutePath());
            commands.add("-classpath");
            commands.add(classpath);
            commands.add(mainClassName);
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    commands.add(args[i]);
                }
            }
            final Slf4jStream stream = Slf4jStream.of(ForkProcessHelper.class);
            return new ProcessExecutor().command(commands)
                    .destroyOnExit()
                    .redirectOutput(stream.asInfo())
                    .redirectError(stream.asWarn())
                    .environment(System.getenv())
                    .stopper(new ProcessStopper() {
                        @Override
                        public void stop(final Process process) {
                            process.destroy();
                        }
                    })
                    .start();
        } catch (final Throwable e) {
            throw Err.process(e);
        }
    }

}