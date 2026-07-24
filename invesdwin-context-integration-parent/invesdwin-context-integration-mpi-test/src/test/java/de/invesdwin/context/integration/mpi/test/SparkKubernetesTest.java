package de.invesdwin.context.integration.mpi.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.MutableBoolean;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.BindMode;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.context.integration.jar.MergedClasspathJar;
import de.invesdwin.context.integration.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.mpi.test.job.SparkJobMain;
import de.invesdwin.context.integration.spark.test.KubernetesContainer;
import de.invesdwin.context.integration.spark.test.SparkContainer;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import de.invesdwin.util.lang.Files;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

@Testcontainers
@NotThreadSafe
public class SparkKubernetesTest extends ATest {

    private static final String SPARK_SA = "spark-sa";
    private static final int NUM_CONTAINERS = 2;
    private static final File LOCAL_LOG_DIR = ContextProperties.getCacheDirectory();
    private static final File JOB_JAR_FILE = newJobJarFile();

    // 1. Spin up a K3s Kubernetes Cluster, mounting the JAR and Log dir into the K8s node
    @Container
    private static final KubernetesContainer K3S = new KubernetesContainer() {
        {
            withFileSystemBind(JOB_JAR_FILE.getAbsolutePath(), "/tmp/job.jar", BindMode.READ_ONLY);
            withFileSystemBind(LOCAL_LOG_DIR.getAbsolutePath(), "/tmp/logs", BindMode.READ_WRITE);
        }
    };

    private static File newJobJarFile() {
        try {
            return new MergedClasspathJar(MergedClasspathJarFilter.DEFAULT, SparkJobMain.class).getResource().getFile();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSparkOnKubernetes() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MutableBoolean jobSuccessful = new MutableBoolean();

        LOCAL_LOG_DIR.mkdirs();
        LOCAL_LOG_DIR.setWritable(true, false);

        // 2. Setup RBAC (Create ServiceAccount and RoleBinding using Fabric8)
        try (KubernetesClient client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(K3S.getKubeConfigYaml()))
                .build()) {

            client.serviceAccounts()
                    .inNamespace("default")
                    .resource(new ServiceAccountBuilder().withNewMetadata().withName(SPARK_SA).endMetadata().build())
                    .serverSideApply();

            client.rbac()
                    .roleBindings()
                    .inNamespace("default")
                    .resource(new RoleBindingBuilder().withNewMetadata()
                            .withName("spark-role-binding")
                            .endMetadata()
                            .withNewRoleRef()
                            .withApiGroup("rbac.authorization.k8s.io")
                            .withKind("ClusterRole")
                            .withName("edit")
                            .endRoleRef()
                            .addNewSubject()
                            .withKind("ServiceAccount")
                            .withName(SPARK_SA)
                            .withNamespace("default")
                            .endSubject()
                            .build())
                    .serverSideApply();
        }

        // 1. Create temporary file for Spark to read KUBECONFIG

        // 2. Safely extract the master URL from the YAML using Fabric8 Config
        final io.fabric8.kubernetes.client.Config k8sConfig = io.fabric8.kubernetes.client.Config
                .fromKubeconfig(K3S.getKubeConfigYaml());
        final String masterUrl = k8sConfig.getMasterUrl(); // e.g., "https://localhost:32768"

        // 3. Configure launcher environment
        final Map<String, String> env = ILockCollectionFactory.getInstance(false).newMap(System.getenv());
        env.put("KUBECONFIG", K3S.getKubeConfigFile().getAbsolutePath());

        final SparkLauncher launcher = new SparkLauncher(env)
                .setSparkHome(SparkContainer.getSparkHomeFolder().getAbsolutePath())
                .setMaster("k8s://" + masterUrl)
                .setDeployMode("cluster")
                .setMainClass(SparkJobMain.class.getName())
                .setAppResource("local:///tmp/job.jar") // 'local://' tells K8s to look inside the pod's file system

                // Configure standard executor limits
                .setConf("spark.executor.instances", String.valueOf(NUM_CONTAINERS))
                .setConf("spark.executor.cores", "1")

                // Use a standard Spark image (must have Java compatible with your JAR)
                .setConf("spark.kubernetes.container.image", SparkContainer.SPARK_IMAGE_NAME)
                .setConf("spark.kubernetes.authenticate.driver.serviceAccountName", SPARK_SA)

                .setConf("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
                .setConf("spark.executor.extraJavaOptions", "-Duser.home=/tmp")

                // 4. Mount the hostPath directories into the Driver and Executor Pods
                // This makes the JAR available at /tmp/job.jar and logs writable to /tmp/logs inside every pod
                .setConf("spark.kubernetes.driver.volumes.hostPath.jobjar.mount.path", "/tmp/job.jar")
                .setConf("spark.kubernetes.driver.volumes.hostPath.jobjar.options.path", "/tmp/job.jar")
                .setConf("spark.kubernetes.executor.volumes.hostPath.jobjar.mount.path", "/tmp/job.jar")
                .setConf("spark.kubernetes.executor.volumes.hostPath.jobjar.options.path", "/tmp/job.jar")

                .setConf("spark.kubernetes.driver.volumes.hostPath.logdir.mount.path", "/tmp/logs")
                .setConf("spark.kubernetes.driver.volumes.hostPath.logdir.options.path", "/tmp/logs")
                .setConf("spark.kubernetes.executor.volumes.hostPath.logdir.mount.path", "/tmp/logs")
                .setConf("spark.kubernetes.executor.volumes.hostPath.logdir.options.path", "/tmp/logs")

                .addAppArgs("--size", String.valueOf(NUM_CONTAINERS), "--logDir", "/tmp/logs", "--hdfsUri", "file:///");

        final SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
            @Override
            public void stateChanged(final SparkAppHandle handle) {
                final State state = handle.getState();
                if (state.isFinal()) {
                    // In K8s cluster mode, the driver pod shutting down severs the SparkLauncher connection,
                    // often causing a LOST state. We accept LOST here and let the subsequent log checks
                    // strictly validate the actual success of the workload.
                    jobSuccessful.set(state == SparkAppHandle.State.FINISHED || state == SparkAppHandle.State.LOST);
                    countDownLatch.countDown();
                }
            }

            @Override
            public void infoChanged(final SparkAppHandle handle) {}
        });

        countDownLatch.await();
        Assertions.checkTrue(jobSuccessful.get(), "Spark on Kubernetes job failed!");
        if (!handle.getState().isFinal()) {
            handle.stop();
        }

        // 5. Verify logs directly from host OS (thanks to volume binding)
        final File log_1_2 = new File(LOCAL_LOG_DIR, "1_2_LatencyServerTask.log");
        final File log_2_2 = new File(LOCAL_LOG_DIR, "2_2_LatencyClientTask.log");

        final String str_1_2 = Files.readFileToStringNoThrow(log_1_2, Charset.defaultCharset());
        final String str_2_2 = Files.readFileToStringNoThrow(log_2_2, Charset.defaultCharset());
        Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
        Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
    }
}