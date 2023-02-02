package de.invesdwin.context.integration.channel.sync;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.BooleanUtils;
import org.zeroturnaround.exec.ProcessExecutor;
import org.zeroturnaround.exec.stop.ProcessStopper;
import org.zeroturnaround.exec.stream.slf4j.Slf4jStream;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.instrument.DynamicInstrumentationProperties;
import de.invesdwin.util.concurrent.lock.ILock;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.time.duration.Duration;

@Immutable
public final class SynchronousChannels {

    public static final Duration DEFAULT_MAX_RECONNECT_DELAY = Duration.ONE_HUNDRED_MILLISECONDS;
    public static final Duration DEFAULT_CONNECT_TIMEOUT = ContextProperties.DEFAULT_NETWORK_TIMEOUT;
    public static final Duration DEFAULT_WAIT_INTERVAL = Duration.ONE_MILLISECOND;

    /**
     * https://stackoverflow.com/questions/1098897/what-is-the-largest-safe-udp-packet-size-on-the-internet
     */
    public static final int MAX_FRAGMENTED_DATAGRAM_PACKET_SIZE = 65507;
    public static final int MAX_UNFRAGMENTED_DATAGRAM_PACKET_SIZE = 508;

    private static final File TMPFS_FOLDER = new File("/dev/shm");
    @GuardedBy("SynchronousChannels.class")
    private static File tmpfsFolderOrFallback;
    @GuardedBy("SynchronousChannels.class")
    private static Boolean namedPipeSupportedCached;

    private SynchronousChannels() {}

    public static <T> ISynchronousReader<T> synchronize(final ISynchronousReader<T> delegate) {
        return new ISynchronousReader<T>() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized T readMessage() throws IOException {
                return delegate.readMessage();
            }

            @Override
            public synchronized boolean hasNext() throws IOException {
                return delegate.hasNext();
            }

            @Override
            public synchronized void readFinished() {
                delegate.readFinished();
            }
        };
    }

    public static <T> ISynchronousWriter<T> synchronize(final ISynchronousWriter<T> delegate) {
        return new ISynchronousWriter<T>() {

            @Override
            public synchronized void close() throws IOException {
                delegate.close();
            }

            @Override
            public synchronized void open() throws IOException {
                delegate.open();
            }

            @Override
            public synchronized boolean writeReady() throws IOException {
                return delegate.writeReady();
            }

            @Override
            public synchronized void write(final T message) throws IOException {
                delegate.write(message);
            }

            @Override
            public synchronized boolean writeFlushed() throws IOException {
                return delegate.writeFlushed();
            }

        };
    }

    public static <T> ISynchronousReader<T> synchronize(final ISynchronousReader<T> delegate, final Object lock) {
        return new ISynchronousReader<T>() {

            @Override
            public void close() throws IOException {
                synchronized (lock) {
                    delegate.close();
                }
            }

            @Override
            public void open() throws IOException {
                synchronized (lock) {
                    delegate.open();
                }
            }

            @Override
            public T readMessage() throws IOException {
                synchronized (lock) {
                    return delegate.readMessage();
                }
            }

            @Override
            public boolean hasNext() throws IOException {
                synchronized (lock) {
                    return delegate.hasNext();
                }
            }

            @Override
            public void readFinished() {
                synchronized (lock) {
                    delegate.readFinished();
                }
            }
        };
    }

    public static <T> ISynchronousWriter<T> synchronize(final ISynchronousWriter<T> delegate, final Object lock) {
        return new ISynchronousWriter<T>() {

            @Override
            public void close() throws IOException {
                synchronized (lock) {
                    delegate.close();
                }
            }

            @Override
            public void open() throws IOException {
                synchronized (lock) {
                    delegate.open();
                }
            }

            @Override
            public boolean writeReady() throws IOException {
                synchronized (lock) {
                    return delegate.writeReady();
                }
            }

            @Override
            public void write(final T message) throws IOException {
                synchronized (lock) {
                    delegate.write(message);
                }
            }

            @Override
            public boolean writeFlushed() throws IOException {
                synchronized (lock) {
                    return delegate.writeFlushed();
                }
            }

        };
    }

    public static <T> ISynchronousReader<T> synchronize(final ISynchronousReader<T> delegate, final ILock lock) {
        return new ISynchronousReader<T>() {

            @Override
            public void close() throws IOException {
                lock.lock();
                try {
                    delegate.close();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void open() throws IOException {
                lock.lock();
                try {
                    delegate.open();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public T readMessage() throws IOException {
                lock.lock();
                try {
                    return delegate.readMessage();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public boolean hasNext() throws IOException {
                lock.lock();
                try {
                    return delegate.hasNext();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void readFinished() {
                lock.lock();
                try {
                    delegate.readFinished();
                } finally {
                    lock.unlock();
                }
            }
        };
    }

    public static <T> ISynchronousWriter<T> synchronize(final ISynchronousWriter<T> delegate, final ILock lock) {
        return new ISynchronousWriter<T>() {

            @Override
            public void close() throws IOException {
                lock.lock();
                try {
                    delegate.close();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void open() throws IOException {
                lock.lock();
                try {
                    delegate.open();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public boolean writeReady() throws IOException {
                lock.lock();
                try {
                    return delegate.writeReady();
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public void write(final T message) throws IOException {
                lock.lock();
                try {
                    delegate.write(message);
                } finally {
                    lock.unlock();
                }
            }

            @Override
            public boolean writeFlushed() throws IOException {
                lock.lock();
                try {
                    return delegate.writeFlushed();
                } finally {
                    lock.unlock();
                }
            }

        };
    }

    public static synchronized File getTmpfsFolderOrFallback() {
        if (tmpfsFolderOrFallback == null) {
            if (TMPFS_FOLDER.exists()) {
                tmpfsFolderOrFallback = DynamicInstrumentationProperties.newTempDirectory(TMPFS_FOLDER);
            } else {
                tmpfsFolderOrFallback = ContextProperties.TEMP_DIRECTORY;
            }
        }
        return tmpfsFolderOrFallback;
    }

    public static synchronized boolean isNamedPipeSupported() {
        if (namedPipeSupportedCached == null) {
            final File namedPipeTestFile = new File(ContextProperties.TEMP_DIRECTORY,
                    SynchronousChannels.class.getSimpleName() + "_NamedPipeTest.pipe");
            namedPipeSupportedCached = namedPipeTestFile.exists() || createNamedPipe(namedPipeTestFile);
            Files.deleteQuietly(namedPipeTestFile);
        }
        return namedPipeSupportedCached;
    }

    public static synchronized boolean createNamedPipe(final File file) {
        if (BooleanUtils.isFalse(namedPipeSupportedCached)) {
            return false;
        }
        try {
            //linux
            execCommand("mkfifo", file.getAbsolutePath());

        } catch (final Exception e) {
            //mac os
            try {
                execCommand("mknod", file.getAbsolutePath());
            } catch (final Exception e2) {
                return false;
            }
        }
        return true;
    }

    private static void execCommand(final String... command) throws Exception {
        new ProcessExecutor().command(command)
                .destroyOnExit()
                .timeout(1, TimeUnit.MINUTES)
                .exitValueNormal()
                .redirectOutput(Slf4jStream.of(SynchronousChannels.class).asInfo())
                .redirectError(Slf4jStream.of(SynchronousChannels.class).asWarn())
                .stopper(new ProcessStopper() {
                    @Override
                    public void stop(final Process process) {
                        process.destroy();
                    }
                })
                .execute();
    }

}
