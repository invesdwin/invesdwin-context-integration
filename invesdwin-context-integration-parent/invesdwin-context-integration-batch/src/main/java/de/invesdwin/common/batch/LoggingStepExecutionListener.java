package de.invesdwin.common.batch;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;

import de.invesdwin.context.log.Log;
import de.invesdwin.util.time.duration.Duration;
import de.invesdwin.util.time.fdate.FDate;
import de.invesdwin.util.time.fdate.FDates;

@ThreadSafe
public class LoggingStepExecutionListener implements StepExecutionListener {

    private final Log log = new Log(this);

    @Override
    public void beforeStep(final StepExecution stepExecution) {
        log.info("Starting step [%s.%s]", stepExecution.getJobExecution().getJobInstance().getJobName(),
                stepExecution.getStepName());
    }

    @Override
    public ExitStatus afterStep(final StepExecution stepExecution) {
        log.info("Finished step [%s.%s] after %s: %s", stepExecution.getJobExecution().getJobInstance().getJobName(),
                stepExecution.getStepName(),
                new Duration(FDate.valueOf(stepExecution.getStartTime()),
                        FDates.min(FDate.valueOf(stepExecution.getEndTime()), new FDate())),
                stepExecution.getSummary());
        return null;
    }
}
