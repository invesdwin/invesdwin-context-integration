package de.invesdwin.context.integration.hadoop;

import java.util.Collection;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;

import org.junit.Test;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.data.hadoop.mapreduce.JobRunner;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;

import de.invesdwin.context.system.properties.SystemProperties;
import de.invesdwin.context.test.ATest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.assertions.Assertions;

@NotThreadSafe
public class HadoopJobTest extends ATest {

    @Inject
    @Named("hadoopTestJobRunner")
    private JobRunner hadoopTestJobRunner;

    @Inject
    @Named("hadoopTestJobCleanupScript")
    private HdfsScriptRunner cleanup;

    @Inject
    private FsShell fsh;

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        ctx.activate(HadoopJobTestContextLocation.class);
    }

    @Test
    public void testRunJob() throws Exception {
        Assertions.assertThat(fsh.test(new SystemProperties().getString("hadoopTestJob.input.path"))).isTrue();
        try {
            hadoopTestJobRunner.call();
            Assertions.assertThat(fsh.test(new SystemProperties().getString("hadoopTestJob.output.path") + "/_SUCCESS"))
                    .isTrue();
            final Collection<String> textCol = fsh
                    .text(new SystemProperties().getString("hadoopTestJob.output.path") + "/part-r-00000");
            Assertions.assertThat(textCol.size()).isEqualTo(1);
            final String text = textCol.iterator().next();
            final String expectedText = "0_mapped_reduced\t[one_mapped]_reduced\n" //
                    + "4_mapped_reduced\t[two_mapped]_reduced\n" //
                    + "8_mapped_reduced\t[three_mapped]_reduced\n";
            Assertions.assertThat(text).isEqualTo(expectedText);
        } finally {
            Assertions.assertThat(cleanup.call()).isNull();
        }
    }
}
