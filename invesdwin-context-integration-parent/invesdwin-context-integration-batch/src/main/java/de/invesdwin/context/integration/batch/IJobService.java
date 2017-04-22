package de.invesdwin.context.integration.batch;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;

public interface IJobService {

    void abandonAllRunningExecutions(String jobName);

    boolean jobExists(String jobName);

    JobExecution startOrResumeJobAndWaitForCompleted(String jobName, TimeUnit pollTimeUnit, long pollTimeout)
            throws InterruptedException;

    JobExecution getLastJobExecution(String jobName);

    JobExecution getLastJobExecution(String jobName, BatchStatus status);

    List<Long> getJobExecutions(String jobName);

}
