package de.invesdwin.context.integration.batch.admin;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.jupiter.api.Test;

import de.invesdwin.context.integration.IntegrationProperties;
import de.invesdwin.context.persistence.jpa.test.APersistenceTest;
import de.invesdwin.context.test.ITestContextSetup;
import de.invesdwin.context.webserver.test.WebserverTest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.lang.string.Strings;
import de.invesdwin.util.lang.uri.URIs;

@NotThreadSafe
//@PersistenceTest(PersistenceTestContext.SERVER)
@WebserverTest
public class BatchAdminWebServerTest extends APersistenceTest {

    @Override
    public void setUpContext(final ITestContextSetup ctx) throws Exception {
        super.setUpContext(ctx);
        ctx.deactivateBean(de.invesdwin.context.integration.batch.internal.DisabledBatchJobTestContext.class);
    }

    @Test
    public void testWebServer() throws InterruptedException {
        final String website = URIs.connect(IntegrationProperties.WEBSERVER_BIND_URI).download();
        Assertions.assertThat(website).contains("Spring Batch Admin");

        final String jobsJson = URIs.connect(IntegrationProperties.WEBSERVER_BIND_URI + "/jobs.json").download();
        //            "registrations" : {
        //                "batchTestJob1" : {
        //                    "name" : "batchTestJob1",
        //                    "resource" : "http://localhost:9001/jobs/batchTestJob1.json",
        //                    "description" : "No description",
        //                    "executionCount" : 0,
        //                    "launchable" : true,
        //                    "incrementable" : true
        //                },
        //                "batchTestJob2" : {
        //                    "name" : "batchTestJob2",
        //                    "resource" : "http://localhost:9001/jobs/batchTestJob2.json",
        //                    "description" : "No description",
        //                    "executionCount" : 0,
        //                    "launchable" : true,
        //                    "incrementable" : true
        //                }
        //             }
        //          }
        //        }
        Assertions.assertThat(jobsJson).contains("\"name\" : \"batchTestJob1\",");
        Assertions.assertThat(jobsJson).contains("\"name\" : \"batchTestJob2\",");
        Assertions.assertThat(jobsJson).contains("\"launchable\" : true,");
        int countLaunchable = 0;
        for (final String line : Strings.splitPreserveAllTokens(jobsJson, "\n")) {
            if (line.contains("\"launchable\" : true,")) {
                countLaunchable++;
            }
        }
        Assertions.assertThat(countLaunchable).isGreaterThanOrEqualTo(2);
        //        TimeUnit.DAYS.sleep(Long.MAX_VALUE);
    }
}
