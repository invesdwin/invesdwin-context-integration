package de.invesdwin.context.integration.ws.registry.internal;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import de.invesdwin.context.integration.ws.registry.IRegistryService;
import de.invesdwin.context.integration.ws.registry.ServiceBinding;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingDao;
import de.invesdwin.context.integration.ws.registry.internal.persistence.ServiceBindingEntity;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.Strings;
import de.invesdwin.util.time.date.FDate;

@ThreadSafe
@Named
public class LocalRegistryService implements IRegistryService {

    @Inject
    private ServiceBindingDao serviceBindingDao;

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Override
    public ServiceBinding registerServiceBinding(final String serviceName, final URI accessUri) throws IOException {
        Assertions.checkNotBlank(serviceName);
        Assertions.checkNotNull(accessUri);
        final ServiceBindingEntity example = ServiceBindingEntity.valueOf(serviceName, accessUri);
        final ServiceBindingEntity existing = serviceBindingDao.findOne(example);
        if (existing != null) {
            existing.setUpdated(new FDate());
            return serviceBindingDao.save(existing).toServiceBinding();
        } else {
            example.setCreated(new FDate());
            example.setUpdated(example.getCreated());
            return serviceBindingDao.save(example).toServiceBinding();
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    @Override
    public ServiceBinding unregisterServiceBinding(final String serviceName, final URI accessUri) throws IOException {
        Assertions.checkNotBlank(serviceName);
        Assertions.checkNotNull(accessUri);
        final ServiceBindingEntity example = ServiceBindingEntity.valueOf(serviceName, accessUri);
        final ServiceBindingEntity existing = serviceBindingDao.findOne(example);
        serviceBindingDao.delete(existing);
        final ServiceBinding deleted = existing.toServiceBinding();
        deleted.setDeleted(new FDate().jodaTimeValue().toDateTime());
        return deleted;
    }

    @Override
    public Collection<ServiceBinding> queryServiceBindings(final String serviceName) throws IOException {
        final List<ServiceBindingEntity> entities;
        if (Strings.isBlank(serviceName)) {
            entities = serviceBindingDao.findAll();
        } else {
            final ServiceBindingEntity example = ServiceBindingEntity.valueOf(serviceName);
            entities = serviceBindingDao.findAll(example);
        }
        final List<ServiceBinding> bindings = new ArrayList<>(entities.size());
        for (final ServiceBindingEntity entity : entities) {
            bindings.add(entity.toServiceBinding());
        }
        return bindings;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

}
