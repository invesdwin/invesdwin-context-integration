package de.invesdwin.context.integration.ws;

import javax.annotation.concurrent.Immutable;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import de.invesdwin.context.integration.ws.registry.publication.XsdWebServicePublicationTest;

@RunWith(Suite.class)
@SuiteClasses({ XsdWebServicePublicationTest.class })
@Immutable
public class WebServiceTestSuite {

}
