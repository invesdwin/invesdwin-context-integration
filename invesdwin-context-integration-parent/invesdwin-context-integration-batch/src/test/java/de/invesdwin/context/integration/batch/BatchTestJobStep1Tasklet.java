package de.invesdwin.context.integration.batch;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Configurable;

import de.invesdwin.util.assertions.Assertions;

@Configurable
@NotThreadSafe
public class BatchTestJobStep1Tasklet implements Tasklet {

    public static final int CALLED_EXPECTED = 3;

    @Inject
    private BatchJobTestBean testBean;

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        Assertions.assertThat(testBean).isNotNull();
        Assertions.assertThat(testBean.test()).isTrue();
        if (chunkContext.getStepContext().getStepExecution().getReadCount() < CALLED_EXPECTED) {
            contribution.incrementReadCount();
            return RepeatStatus.CONTINUABLE;
        } else {
            return RepeatStatus.FINISHED;
        }
    }

}
