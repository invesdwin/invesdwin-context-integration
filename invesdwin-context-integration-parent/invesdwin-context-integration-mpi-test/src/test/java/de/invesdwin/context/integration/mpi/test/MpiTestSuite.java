package de.invesdwin.context.integration.mpi.test;

import javax.annotation.concurrent.Immutable;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

@Suite
@SelectClasses({ FastMpjTest.class, MpjExpressTest.class, MpjExpressYarnTest.class, OpenMpiTest.class })
@Immutable
public class MpiTestSuite {

}
