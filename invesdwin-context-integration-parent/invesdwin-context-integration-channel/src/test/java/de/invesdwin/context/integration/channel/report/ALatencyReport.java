package de.invesdwin.context.integration.channel.report;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.html.distribution.DistributionMeasure;
import de.invesdwin.context.integration.html.distribution.HtmlDistributionReport;
import de.invesdwin.util.collections.Arrays;
import de.invesdwin.util.collections.iterable.ICloseableIterable;
import de.invesdwin.util.collections.iterable.ICloseableIterator;
import de.invesdwin.util.lang.Closeables;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.time.date.FDate;
import de.invesdwin.util.time.date.FTimeUnit;
import de.invesdwin.util.time.date.IFDateProvider;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;

@NotThreadSafe
public abstract class ALatencyReport implements ILatencyReport {

    private static String folderNameOverride = null;
    private static final String CSV_SEPARATOR = "\t";

    private final IFDateProvider messageProvider = new IFDateProvider() {
        @Override
        public FDate asFDate() {
            return new FDate(newTimestamp());
        }

    };

    private final String name;
    private final File file;
    private final OutputStream out;

    public ALatencyReport(final String name) {
        this.name = name;
        this.file = newFile(name);
        try {
            Files.forceMkdirParent(file);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        try {
            out = new FastBufferedOutputStream(new FileOutputStream(file));
        } catch (final FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        try {
            final String headerLine = "index" + CSV_SEPARATOR + newMeasureTimeUnit().getShortName() + "\n";
            out.write(headerLine.getBytes());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setFolderNameOverride(final String folderNameOverride) {
        ALatencyReport.folderNameOverride = folderNameOverride;
    }

    public static String getFolderNameOverride() {
        return folderNameOverride;
    }

    protected abstract FTimeUnit newMeasureTimeUnit();

    protected FTimeUnit newReportTimeUnit() {
        return FTimeUnit.MICROSECONDS;
    }

    protected File newFile(final String name) {
        return new File(newFolder(), name + ".csv");
    }

    protected File newFolder() {
        if (folderNameOverride != null) {
            return new File(newBaseFolder(), folderNameOverride);
        } else {
            return new File(newBaseFolder(), getClass().getSimpleName());
        }
    }

    protected File newBaseFolder() {
        return ContextProperties.getLogDirectory();
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
    public void measureLatency(final int index, final FDate message, final FDate arrivalTimestamp) {
        try {
            final String line = index + CSV_SEPARATOR + (arrivalTimestamp.millisValue() - message.millisValue()) + "\n";
            out.write(line.getBytes());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
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
    public void validateOrder(final FDate prevValue, final FDate nextValue) {
        //noop
    }

    @Override
    public void close() {
        Closeables.closeQuietly(out);
        final List<Decimal> values = new ArrayList<>();
        final FTimeUnit measureTimeUnit = newMeasureTimeUnit();
        final FTimeUnit reportTimeUnit = newReportTimeUnit();
        try {
            final int decimalPlaces = Decimal.valueOf(measureTimeUnit.convert(1, reportTimeUnit))
                    .getWholeNumberDigits();
            final List<String> lines = Files.readAllLines(file.toPath());
            for (int i = 1; i < lines.size(); i++) {
                final String line = lines.get(i);
                final String[] tokens = line.split(CSV_SEPARATOR);
                final int index = Integer.parseInt(tokens[0]);
                if (index < 0) {
                    continue;
                }
                final long latencyValueRaw = Long.parseLong(tokens[1]);
                final double latencyValue = reportTimeUnit.asFractional()
                        .convert(latencyValueRaw, measureTimeUnit.asFractional());
                values.add(Decimal.valueOf(latencyValue).round(decimalPlaces));
            }
            if (!values.isEmpty()) {
                final File htmlFile = new File(newFolder(), name + ".html");
                final DistributionMeasure measure = new DistributionMeasure(name, reportTimeUnit.getShortName(), values,
                        false, decimalPlaces);
                new HtmlDistributionReport().writeReport(htmlFile, Arrays.asList(measure));
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

}
