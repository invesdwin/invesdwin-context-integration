package de.invesdwin.context.integration.ws;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

import de.invesdwin.context.integration.ws.registry.publication.XsdWebServicePublicationTest;

@Suite
@SelectClasses({ XsdWebServicePublicationTest.class })
@Immutable
public class WebServiceTestSuite {

}
