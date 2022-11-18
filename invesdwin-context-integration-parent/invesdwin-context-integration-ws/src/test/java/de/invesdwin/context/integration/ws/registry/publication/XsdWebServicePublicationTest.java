package de.invesdwin.context.integration.ws.registry.publication;

import javax.annotation.concurrent.Immutable;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.integration.ws.registry.publication.internal.WebServicePublicationManager;
import de.invesdwin.context.test.ATest;
import jakarta.inject.Inject;

@Immutable
public final class XsdWebServicePublicationTest extends ATest {

    @Inject
    private WebServicePublicationManager manager;

    @Test
    public void testPublication() throws Exception {
        final XsdWebServicePublication publication = new XsdWebServicePublication();
        publication.setBeanName("ws.test");
        publication.setUseRegistry(true);
        MergedContext.autowire(publication);
        publication.afterPropertiesSet();
        manager.registerPublication(publication);
        manager.unregisterAllPublications();
    }

}
