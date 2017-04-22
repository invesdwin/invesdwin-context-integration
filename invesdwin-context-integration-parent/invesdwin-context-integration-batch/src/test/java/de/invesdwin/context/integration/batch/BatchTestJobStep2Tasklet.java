package de.invesdwin.context.integration.batch;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

@NotThreadSafe
public class BatchTestJobStep2Tasklet implements Tasklet {

    public static final int CALLED_EXPECTED = 1;

    @Override
    public RepeatStatus execute(final StepContribution contribution, final ChunkContext chunkContext) throws Exception {
        contribution.incrementReadCount();
        return RepeatStatus.FINISHED;
    }

}
