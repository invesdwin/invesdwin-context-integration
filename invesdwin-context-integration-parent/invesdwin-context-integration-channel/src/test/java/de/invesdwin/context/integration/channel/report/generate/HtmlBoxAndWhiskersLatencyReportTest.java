package de.invesdwin.context.integration.channel.report.generate;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.html.distribution.DistributionMeasure;
import de.invesdwin.context.integration.html.distribution.box.HtmlDistributionBoxAndWhiskerReport;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.lang.Files;
import de.invesdwin.util.math.decimal.Decimal;
import de.invesdwin.util.time.date.FTimeUnit;

@NotThreadSafe
public class HtmlBoxAndWhiskersLatencyReportTest extends ATest {

    private static final String CSV_SEPARATOR = "\t";

    @Test
    public void generate() throws IOException {
        final FTimeUnit reportTimeUnit = FTimeUnit.MILLISECONDS;

        final File baseFolder = new File(ContextProperties.getLogDirectory(),
                HtmlBoxAndWhiskersLatencyReportTest.class.getSimpleName());

        final File file = new File(baseFolder, "BoxAndWhiskers.html");

        final File samplesFolder = new File(baseFolder, "samples");

        final Map<String, List<DistributionMeasure>> measure_samples = new LinkedHashMap<>();
        final List<DistributionMeasure> samples = new ArrayList<>();

        final File[] fileObjects = samplesFolder.listFiles();

        for (final File csvFile : fileObjects) {
            final List<String> lines = Files.readAllLines(csvFile.toPath());
            final List<Decimal> measures = new ArrayList<>();
            if (lines.isEmpty()) {
                log.info(csvFile.getName() + ": skipped empty");
                continue;
            }
            final String firstLine = lines.get(0);
            final String[] firstLineTokens = firstLine.split(CSV_SEPARATOR);
            if (firstLineTokens.length != 2) {
                if (lines.isEmpty()) {
                    log.info(csvFile.getName() + ": skipped not two columns: " + firstLineTokens.length);
                    continue;
                }
            }
            final FTimeUnit measureTimeUnit = FTimeUnit.valueOfAlias(firstLineTokens[1]);
            final int decimalPlaces = Decimal.valueOf(measureTimeUnit.convert(1, reportTimeUnit))
                    .getWholeNumberDigits();
            for (int i = 1; i < lines.size(); i++) {
                final String line = lines.get(i);
                final String[] tokens = line.split(CSV_SEPARATOR);
                try {
                    final int index = Integer.parseInt(tokens[0]);
                    if (index < 0) {
                        continue;
                    }
                    final long latencyValueRaw = Long.parseLong(tokens[1]);
                    final double latencyValue = reportTimeUnit.asFractional()
                            .convert(latencyValueRaw, measureTimeUnit.asFractional());
                    measures.add(Decimal.valueOf(latencyValue).round(decimalPlaces));
                } catch (final NumberFormatException e) {
                    throw new RuntimeException(e);
                }

            }
            log.info(csvFile.getName() + ": " + measures.size());
            Collections.sort(measures);
            //final int removeLength = (int) (measures.size() * new Percent(5, PercentScale.PERCENT).getRate());
            //Lists.removeRange(measures, measures.size() - removeLength, measures.size());
            final String sampleName = Files.removeExtension(csvFile.getName());
            final DistributionMeasure measure = new DistributionMeasure(sampleName, "ms", measures, false, 2);
            samples.add(measure);
        }
        measure_samples.put("Latency", samples);
        new HtmlDistributionBoxAndWhiskerReport().writeReport(file, measure_samples);
    }
}