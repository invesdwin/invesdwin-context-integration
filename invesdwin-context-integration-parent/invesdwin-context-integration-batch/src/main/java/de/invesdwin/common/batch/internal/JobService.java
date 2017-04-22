package de.invesdwin.common.batch.internal;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;

import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.batch.core.launch.JobExecutionNotRunningException;
import org.springframework.batch.core.launch.JobParametersNotFoundException;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.core.launch.NoSuchJobExecutionException;
import org.springframework.batch.core.launch.NoSuchJobInstanceException;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.core.step.StepLocator;

import de.invesdwin.common.batch.IJobService;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.time.fdate.FDate;

@ThreadSafe
@Named
public class JobService implements IJobService {

    @Inject
    private org.springframework.batch.core.launch.JobOperator jobOperator;
    @Inject
    private org.springframework.batch.core.explore.JobExplorer jobExplorer;
    @Inject
    private org.springframework.batch.core.configuration.JobRegistry jobRegistry;
    @Inject
    private org.springframework.batch.core.repository.JobRepository jobRepository;

    @Override
    public JobExecution startOrResumeJobAndWaitForCompleted(final String jobName, final TimeUnit pollTimeUnit,
            final long pollTimeout) throws InterruptedException {
        try {
            final long executionId;
            final JobExecution lastJobExecution = getLastJobExecution(jobName);
            if (lastJobExecution != null
                    && (lastJobExecution.isRunning() || lastJobExecution.getStatus() == BatchStatus.UNKNOWN
                            || lastJobExecution.getStatus() == BatchStatus.FAILED)) {
                updateUnknownStepExecutionsToFailed(lastJobExecution);
                lastJobExecution.setEndTime(new FDate().dateValue());
                lastJobExecution.setStatus(BatchStatus.FAILED);
                jobRepository.update(lastJobExecution);
                executionId = jobOperator.restart(lastJobExecution.getId());
            } else {
                executionId = jobOperator.startNextInstance(jobName);
            }
            while (jobOperator.getRunningExecutions(jobName).contains(executionId)) {
                pollTimeUnit.sleep(pollTimeout);
            }
            final JobExecution jobExecution = jobExplorer.getJobExecution(executionId);
            Assertions.assertThat(jobExecution.getStatus()).isEqualTo(BatchStatus.COMPLETED);
            return jobExecution;
        } catch (final NoSuchJobException | JobParametersNotFoundException | JobRestartException
                | JobExecutionAlreadyRunningException | JobInstanceAlreadyCompleteException
                | UnexpectedJobExecutionException | JobParametersInvalidException | NoSuchJobExecutionException e) {
            throw Err.process(e);
        }
    }

    private void updateUnknownStepExecutionsToFailed(final JobExecution lastJobExecution) {
        final Collection<String> stepNames = getStepNames(lastJobExecution.getJobInstance().getJobName());
        for (final String stepName : stepNames) {
            final StepExecution lastStepExecution = jobRepository
                    .getLastStepExecution(lastJobExecution.getJobInstance(), stepName);
            if (lastStepExecution != null && lastStepExecution.getStatus() == BatchStatus.UNKNOWN) {
                //UNKOWN step executions cannot be restarted, thus change it
                lastStepExecution.setStatus(BatchStatus.FAILED);
                jobRepository.update(lastStepExecution);
            }
        }
    }

    private Collection<String> getStepNames(final String jobName) {
        try {
            final Job job = jobRegistry.getJob(jobName);
            final StepLocator stepLocator = (StepLocator) job;
            final Collection<String> stepNames = stepLocator.getStepNames();
            return stepNames;
        } catch (final NoSuchJobException e) {
            throw Err.process(e);
        }
    }

    @Override
    public boolean jobExists(final String jobName) {
        return jobRegistry.getJobNames().contains(jobName);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Long> getJobExecutions(final String jobName) {
        try {
            final List<Long> jobInstances = jobOperator.getJobInstances(jobName, 0, 1);
            if (jobInstances.size() == 0) {
                return Collections.EMPTY_LIST;
            }
            Assertions.assertThat(jobInstances.size()).isEqualTo(1);
            final List<Long> executions = jobOperator.getExecutions(jobInstances.get(0));
            return executions;
        } catch (final NoSuchJobInstanceException | NoSuchJobException e) {
            throw Err.process(e);
        }
    }

    @Override
    public JobExecution getLastJobExecution(final String jobName) {
        final List<Long> executions = getJobExecutions(jobName);
        if (executions.size() == 0) {
            return null;
        }
        final JobExecution jobExecution = jobExplorer.getJobExecution(executions.get(0));
        return jobExecution;
    }

    @Override
    public JobExecution getLastJobExecution(final String jobName, final BatchStatus status) {
        final List<Long> executions = getJobExecutions(jobName);
        if (executions.size() == 0) {
            return null;
        }
        for (final Long execution : executions) {
            final JobExecution jobExecution = jobExplorer.getJobExecution(execution);
            if (jobExecution.getStatus().equals(status)) {
                return jobExecution;
            }
        }
        return null;
    }

    @Override
    public void abandonAllRunningExecutions(final String jobName) {
        final Set<JobExecution> jobExecutions = jobExplorer.findRunningJobExecutions(jobName);
        try {
            for (final JobExecution jobExecution : jobExecutions) {
                final boolean stopped = jobOperator.stop(jobExecution.getId());
                Assertions.assertThat(stopped).isTrue();
                jobOperator.abandon(jobExecution.getId());
            }
        } catch (NoSuchJobExecutionException | JobExecutionAlreadyRunningException
                | JobExecutionNotRunningException e) {
            throw Err.process(e);
        }
    }

}
