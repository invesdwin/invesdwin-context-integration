package de.invesdwin.context.integration.mpi.test.job;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.log.error.Err;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.lang.string.Strings;

@NotThreadSafe
public final class MpiJobYarnMain {

    private MpiJobYarnMain() {}

    public static void main(final String[] args) {
        final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        StringBuilder classpath = new StringBuilder();
        for (final URL url : DynamicInstrumentationReflections.getURLs(classLoader)) {
            classpath.append(url.toString());
            classpath.append(File.pathSeparator);
        }
        classpath = Strings.removeEnd(classpath, ":");
        try {
            //upgrade to java 17 from java 11 (hadoop does not support java 17 yet)
            final List<String> commands = new ArrayList<>();
            commands.add("/usr/lib/jvm/java-17-openjdk-amd64/bin/java");
            commands.add("-classpath");
            commands.add(classpath.toString());
            commands.add(MpiJobMain.class.getName());
            for (int i = 0; i < args.length; i++) {
                commands.add(args[i]);
            }
            new ProcessExecutor().command(commands)
                    .destroyOnExit()
                    .exitValueNormal()
                    .redirectOutput(Slf4jStream.of(MpiJobYarnMain.class).asInfo())
                    .redirectError(Slf4jStream.of(MpiJobYarnMain.class).asWarn())
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

}
