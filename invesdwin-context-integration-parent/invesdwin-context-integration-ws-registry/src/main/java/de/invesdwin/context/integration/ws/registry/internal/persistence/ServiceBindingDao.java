package de.invesdwin.context.integration.ws.registry.internal.persistence;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Named;

import de.invesdwin.context.persistence.jpa.api.dao.ADao;

@Named
@ThreadSafe
public class ServiceBindingDao extends ADao<ServiceBindingEntity> {

}
