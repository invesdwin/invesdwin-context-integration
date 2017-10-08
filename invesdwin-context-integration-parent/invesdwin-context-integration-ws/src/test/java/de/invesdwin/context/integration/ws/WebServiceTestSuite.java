package de.invesdwin.context.integration.ws;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;

import de.invesdwin.context.integration.ws.registry.publication.XsdWebServicePublicationTest;

@RunWith(JUnitPlatform.class)
@SelectClasses({ XsdWebServicePublicationTest.class })
@Immutable
public class WebServiceTestSuite {

}
