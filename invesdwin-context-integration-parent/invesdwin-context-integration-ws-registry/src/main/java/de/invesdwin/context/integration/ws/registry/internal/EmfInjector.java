package de.invesdwin.context.integration.ws.registry.internal;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;
import javax.inject.Named;
import javax.persistence.EntityManagerFactory;

import org.apache.juddi.config.PersistenceManager;
import org.springframework.beans.factory.InitializingBean;

import de.invesdwin.context.persistence.jpa.PersistenceProperties;
import de.invesdwin.util.lang.Reflections;

@NotThreadSafe
public class EmfInjector implements InitializingBean {

    @Inject
    @Named(PersistenceProperties.DEFAULT_ENTITY_MANAGER_FACTORY_NAME)
    private EntityManagerFactory emf;

    private static void injectEMF(final EntityManagerFactory emf) {
        Reflections.field("emf").ofType(EntityManagerFactory.class).in(PersistenceManager.class).set(emf);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        injectEMF(emf);
    }

}
