package de.invesdwin.context.integration.batch.admin.internal;

import java.util.Collection;

import javax.annotation.concurrent.ThreadSafe;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.JobFactory;
import org.springframework.batch.core.launch.NoSuchJobException;

import de.invesdwin.context.beans.init.MergedContext;

@ThreadSafe
public class ParentDelegateJobRegistry implements org.springframework.batch.core.configuration.JobRegistry {

    @Override
    public Collection<String> getJobNames() {
        return getDelegate().getJobNames();
    }

    @Override
    public Job getJob(final String name) throws NoSuchJobException {
        return getDelegate().getJob(name);
    }

    @Override
    public void register(final JobFactory jobFactory) throws DuplicateJobException {
        getDelegate().register(jobFactory);
    }

    @Override
    public void unregister(final String jobName) {
        getDelegate().unregister(jobName);
    }

    private org.springframework.batch.core.configuration.JobRegistry getDelegate() {
        return MergedContext.getInstance().getBean(org.springframework.batch.core.configuration.JobRegistry.class);
    }

}
