package de.invesdwin.context.integration.batch.internal;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.batch.core.configuration.support.AutomaticJobRegistrar;
import org.springframework.core.io.Resource;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.log.Log;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.util.lang.Resources;

@NotThreadSafe
public class LoggingAutomaticJobRegistrar extends AutomaticJobRegistrar {

    private final Log log = new Log(this);

    private org.springframework.batch.core.configuration.JobRegistry jobRegistry;
    private String jobContextResourcePattern;

    public void setJobRegistry(final org.springframework.batch.core.configuration.JobRegistry jobRegistry) {
        this.jobRegistry = jobRegistry;
    }

    public void setJobContextResourcePattern(final String jobContextResourcePattern) {
        this.jobContextResourcePattern = jobContextResourcePattern;
    }

    @Override
    public void start() {
        super.start();
        if (log.isInfoEnabled()) {
            final Collection<String> jobNames = jobRegistry.getJobNames();
            if (jobNames.size() > 0) {
                String jobSingularPlural = "job";
                if (jobNames.size() != 1) {
                    jobSingularPlural += "s";
                }

                try {
                    final Resource[] jobContextResources = MergedContext.getInstance()
                            .getResources(jobContextResourcePattern);
                    final List<String> resourceNames = Resources
                            .extractMetaInfResourceLocations(Arrays.asList(jobContextResources));

                    log.info("Found %s batch %s %s in %s", jobNames.size(), jobSingularPlural, jobNames, resourceNames);
                } catch (final IOException e) {
                    throw Err.process(e);
                }
            }
        }
    }

}
