package de.invesdwin.context.integration.channel.report;

import java.io.File;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.IFDateProvider;

@NotThreadSafe
public abstract class ALatencyReport implements ILatencyReport {

    private final IFDateProvider messageProvider = new IFDateProvider() {
        @Override
        public FDate asFDate() {
            return new FDate(newTimestamp());
        }

    };

    private final File file;

    public ALatencyReport(final String name) {
        this.file = newFile(name);
    }

    protected File newFile(final String name) {
        return new File(newFolder(), name + ".txt");
    }

    protected File newFolder() {
        return new File(newBaseFolder(), getClass().getSimpleName());
    }

    protected File newBaseFolder() {
        return ContextProperties.getCacheDirectory();
    }

    protected abstract long newTimestamp();

    @Override
    public IFDateProvider newArrivalTimestamp() {
        return messageProvider;
    }

    @Override
    public ICloseableIterable<? extends IFDateProvider> newRequestMessages() {
        return new ICloseableIterable<IFDateProvider>() {
            @Override
            public ICloseableIterator<IFDateProvider> iterator() {
                return new ICloseableIterator<IFDateProvider>() {

                    @Override
                    public IFDateProvider next() {
                        return messageProvider;
                    }

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    @Override
    public IFDateProvider newResponseMessage(final FDate request) {
        return messageProvider;
    }

    @Override
    public void measureLatency(final FDate message, final FDate arrivalTimestamp) {
        //ystem.out.println("TODO: measure latency");
    }

    @Override
    public boolean isMeasuringLatency() {
        return true;
    }

    @Override
    public void validateResponse(final FDate request, final FDate response) {
        //noop
    }

    @Override
    public void close() {
        //ystem.out.println("TODO: finish file");
    }

}
