package de.invesdwin.context.integration.batch.internal;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.DuplicateJobException;
import org.springframework.batch.core.configuration.support.ApplicationContextFactory;
import org.springframework.batch.core.configuration.support.DefaultJobLoader;
import org.springframework.core.io.Resource;

import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.context.integration.batch.IDisabledBatchContext;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.reflection.Reflections;

@NotThreadSafe
public class BatchContextIgnoringJobLoader extends DefaultJobLoader {

    private Set<String> disabledBatchContextResourceNames;

    @Override
    public void afterPropertiesSet() {
        super.afterPropertiesSet();
        final Map<String, IDisabledBatchContext> beansOfType = PreMergedContext.getInstance()
                .getBeansOfType(IDisabledBatchContext.class);
        disabledBatchContextResourceNames = ILockCollectionFactory.getInstance(false).newSet();
        for (final IDisabledBatchContext bean : beansOfType.values()) {
            final Set<String> resourceNames = bean.getResourceNames();
            if (resourceNames != null) {
                disabledBatchContextResourceNames.addAll(resourceNames);
            }
        }
    }

    @Override
    public Collection<Job> load(final ApplicationContextFactory factory) throws DuplicateJobException {
        final Object[] resourcesObj = Reflections.field("resources").ofType(Object[].class).in(factory).get();
        Assertions.assertThat(resourcesObj).hasSize(1);
        final Object resourceObj = resourcesObj[0];
        Assertions.assertThat(resourceObj).isInstanceOf(Resource.class);
        final Resource resource = (Resource) resourceObj;
        if (disabledBatchContextResourceNames.contains(resource.getFilename())) {
            return Collections.emptyList();
        }
        return super.load(factory);
    }
}
