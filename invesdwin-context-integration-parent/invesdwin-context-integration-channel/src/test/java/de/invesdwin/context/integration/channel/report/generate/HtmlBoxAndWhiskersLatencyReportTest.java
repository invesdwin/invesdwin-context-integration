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

@NotThreadSafe
public class HtmlBoxAndWhiskersLatencyReportTest extends ATest {

    @Test
    public void generate() throws IOException {
        final File folder = new File(ContextProperties.getLogDirectory(),
                HtmlBoxAndWhiskersLatencyReportTest.class.getSimpleName());
        final File file = new File(folder, "BoxAndWhiskers.html");
        final Map<String, List<DistributionMeasure>> measure_samples = new LinkedHashMap<>();
        final List<DistributionMeasure> samples = new ArrayList<>();
        //TODO angelo: read in a sample for each file that should be added to the box and whiskers plot based
        measure_samples.put("Latency", samples);
        new HtmlDistributionBoxAndWhiskerReport().writeReport(file, measure_samples);
    }

}
