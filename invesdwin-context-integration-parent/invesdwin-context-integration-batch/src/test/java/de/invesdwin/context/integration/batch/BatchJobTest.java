package de.invesdwin.context.integration.batch;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;
import jakarta.inject.Inject;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;

import de.invesdwin.context.integration.batch.internal.DisabledBatchJobTestContext;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.context.test.TestContext;
import de.invesdwin.util.assertions.Assertions;

// @ContextConfiguration(inheritLocations = false, locations = { APersistenceTest.CTX_TEST_SERVER })
@NotThreadSafe
public class BatchJobTest extends APersistenceTest {

    @Inject
    private IJobService jobService;

    @Override
    public void setUpContext(final TestContext ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(DisabledBatchJobTestContext.class);
    }

    @Test
    public void testJob() throws Exception {
        testJob("batchTestJob1");
        testJob("batchTestJob2");
    }

    private void testJob(final String jobName) throws Exception {
        Assertions.assertThat(jobService.jobExists(jobName)).isTrue();
        final JobExecution jobExecution = jobService.startOrResumeJobAndWaitForCompleted(jobName, TimeUnit.SECONDS, 3);
        log.info("%s", jobExecution);
        Assertions.assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
        final Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        Assertions.assertThat(stepExecutions.size()).isEqualTo(2);
        final Iterator<StepExecution> iterator = stepExecutions.iterator();
        Assertions.assertThat(iterator.next().getReadCount()).isEqualTo(BatchTestJobStep1Tasklet.CALLED_EXPECTED);
        Assertions.assertThat(iterator.next().getReadCount()).isEqualTo(BatchTestJobStep2Tasklet.CALLED_EXPECTED);
    }
}
