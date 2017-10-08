package de.invesdwin.context.integration;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.platform.suite.api.SelectClasses;
import org.junit.runner.RunWith;

import de.invesdwin.context.integration.csv.CsvItemReaderBuilderTest;
import de.invesdwin.context.integration.csv.CsvVerificationTest;
import de.invesdwin.context.integration.network.NetworkUtilTest;
import de.invesdwin.context.integration.streams.DecompressingInputStreamTest;

@RunWith(JUnitPlatform.class)
@SelectClasses({ CsvVerificationTest.class, DecompressingInputStreamTest.class, MarshallersTest.class,
        CsvItemReaderBuilderTest.class, IntegrationTest.class, NetworkUtilTest.class })
@Immutable
public class IntegrationTestSuite {

}
