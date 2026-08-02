package de.invesdwin.context.integration.grid.spark.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.annotation.concurrent.NotThreadSafe;

import org.agrona.collections.MutableBoolean;
import org.apache.commons.io.IOUtils;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container.ExecResult;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.MountableFile;

import de.invesdwin.context.integration.grid.jar.MergedClasspathJar;
import de.invesdwin.context.integration.grid.jar.visitor.MergedClasspathJarFilter;
import de.invesdwin.context.integration.grid.spark.test.job.SparkJobMain;
import de.invesdwin.context.test.ATest;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;
import io.fabric8.kubernetes.api.model.PersistentVolumeClaimBuilder;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ServiceAccountBuilder;
import io.fabric8.kubernetes.api.model.rbac.RoleBindingBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;

@Testcontainers
@NotThreadSafe
public class SparkKubernetesImageTest extends ATest {

    private static final String SPARK_SA = "spark-sa";
    private static final String SHARED_PVC = "spark-pvc";
    private static final String HELPER_POD = "pvc-helper";
    private static final int NUM_CONTAINERS = 2;

    // 1. Build custom Docker image on-the-fly with the application JAR baked in
    private static final String JOB_SPARK_IMAGE = newJobSparkImage();

    @Container
    private static final KubernetesContainer K3S = new KubernetesContainer();

    private static String newJobSparkImage() {
        try {
            final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.DEFAULT, SparkJobMain.class)
                    .getResource()
                    .getFile();
            return new ImageFromDockerfile().withFileFromFile("job.jar", jobJarFile)
                    .withFileFromString("Dockerfile",
                            "FROM " + SparkContainer.SPARK_IMAGE_NAME + "\n" + "COPY job.jar /opt/spark/job.jar\n")
                    .get();
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSparkOnKubernetesImage() throws Exception {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final MutableBoolean jobSuccessful = new MutableBoolean();

        try (KubernetesClient client = new KubernetesClientBuilder()
                .withConfig(Config.fromKubeconfig(K3S.getKubeConfigYaml()))
                .build()) {

            uploadJobSparkImage();

            // 2. Setup RBAC (ServiceAccount & RoleBinding)
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

            // 3. Create PVC solely for collection of runtime log outputs
            client.persistentVolumeClaims()
                    .inNamespace("default")
                    .resource(new PersistentVolumeClaimBuilder().withNewMetadata()
                            .withName(SHARED_PVC)
                            .endMetadata()
                            .withNewSpec()
                            .withAccessModes("ReadWriteOnce")
                            .withNewResources()
                            .addToRequests("storage", new Quantity("1Gi"))
                            .endResources()
                            .endSpec()
                            .build())
                    .serverSideApply();

            // 4. Spawn a helper pod mounting the PVC to read back task logs after completion
            client.pods()
                    .inNamespace("default")
                    .resource(new PodBuilder().withNewMetadata()
                            .withName(HELPER_POD)
                            .endMetadata()
                            .withNewSpec()
                            .addNewContainer()
                            .withName("helper")
                            .withImage(SparkContainer.SPARK_IMAGE_NAME)
                            .withCommand("sh", "-c", "trap : TERM INT; sleep infinity & wait")
                            .addNewVolumeMount()
                            .withName("shared-vol")
                            .withMountPath("/mnt/shared")
                            .endVolumeMount()
                            .endContainer()
                            .addNewVolume()
                            .withName("shared-vol")
                            .withNewPersistentVolumeClaim()
                            .withClaimName(SHARED_PVC)
                            .endPersistentVolumeClaim()
                            .endVolume()
                            .endSpec()
                            .build())
                    .serverSideApply();

            client.pods().inNamespace("default").withName(HELPER_POD).waitUntilReady(1, TimeUnit.MINUTES);

            final Config k8sConfig = Config.fromKubeconfig(K3S.getKubeConfigYaml());
            final String masterUrl = k8sConfig.getMasterUrl();

            final Map<String, String> env = ILockCollectionFactory.getInstance(false).newMap(System.getenv());
            env.put("KUBECONFIG", K3S.getKubeConfigFile().getAbsolutePath());

            //alterantively, we can upload the JAR to the helper pod and reference it from there, but in this case we baked it into the image
            //            final File jobJarFile = new MergedClasspathJar(MergedClasspathJarFilter.DEFAULT, SparkJobMain.class).getResource()
            //                    .getFile();
            //            client.pods()
            //                    .inNamespace("default")
            //                    .withName(HELPER_POD)
            //                    .file("/mnt/shared/job.jar")
            //                    .upload(jobJarFile.toPath());

            // 5. Launch Spark job referencing the custom image and internal local JAR path
            final SparkLauncher launcher = new SparkLauncher(env)
                    .setSparkHome(SparkContainer.getSparkHomeFolder().getAbsolutePath())
                    .setMaster("k8s://" + masterUrl)
                    .setDeployMode("cluster")
                    .setMainClass(SparkJobMain.class.getName())
                    .setAppResource("local:///opt/spark/job.jar")

                    .setConf("spark.executor.instances", String.valueOf(NUM_CONTAINERS))
                    .setConf("spark.executor.cores", "1")
                    .setConf("spark.kubernetes.container.image", JOB_SPARK_IMAGE)

                    // Crucial: Tell Kubernetes not to try to resolve the local Testcontainers image name remotely
                    .setConf("spark.kubernetes.container.image.pullPolicy", "Never")

                    .setConf("spark.kubernetes.authenticate.driver.serviceAccountName", SPARK_SA)

                    .setConf("spark.driver.extraJavaOptions", "-Duser.home=/tmp")
                    .setConf("spark.executor.extraJavaOptions", "-Duser.home=/tmp")

                    // Mount the PVC solely for application logs
                    .setConf("spark.kubernetes.driver.volumes.persistentVolumeClaim.shared-vol.options.claimName",
                            SHARED_PVC)
                    .setConf("spark.kubernetes.driver.volumes.persistentVolumeClaim.shared-vol.mount.path",
                            "/mnt/shared")
                    .setConf("spark.kubernetes.executor.volumes.persistentVolumeClaim.shared-vol.options.claimName",
                            SHARED_PVC)
                    .setConf("spark.kubernetes.executor.volumes.persistentVolumeClaim.shared-vol.mount.path",
                            "/mnt/shared")

                    .addAppArgs("--size", String.valueOf(NUM_CONTAINERS), "--logDir", "file:///mnt/shared/logs");

            final SparkAppHandle handle = launcher.startApplication(new SparkAppHandle.Listener() {
                @Override
                public void stateChanged(final SparkAppHandle handle) {
                    final State state = handle.getState();
                    if (state.isFinal()) {
                        jobSuccessful.set(state == SparkAppHandle.State.FINISHED || state == SparkAppHandle.State.LOST);
                        countDownLatch.countDown();
                    }
                }

                @Override
                public void infoChanged(final SparkAppHandle handle) {}
            });

            countDownLatch.await();
            Assertions.checkTrue(jobSuccessful.get(), "Spark on Kubernetes image test failed!");
            if (!handle.getState().isFinal()) {
                handle.stop();
            }

            // 6. Fetch and verify task logs written to the PVC storage path
            final String str_1_2;
            try (InputStream is = client.pods()
                    .inNamespace("default")
                    .withName(HELPER_POD)
                    .file("/mnt/shared/logs/1_2_LatencyServerTask.log")
                    .read()) {
                str_1_2 = IOUtils.toString(is, Charset.defaultCharset());
            }

            final String str_2_2;
            try (InputStream is = client.pods()
                    .inNamespace("default")
                    .withName(HELPER_POD)
                    .file("/mnt/shared/logs/2_2_LatencyClientTask.log")
                    .read()) {
                str_2_2 = IOUtils.toString(is, Charset.defaultCharset());
            }

            Assertions.assertThat(str_1_2).contains("WritesFinished: ").contains("(100%)");
            Assertions.assertThat(str_2_2).contains("ReadsFinished: ").contains("(100%)");
        }
    }

    private void uploadJobSparkImage() throws IOException, FileNotFoundException, InterruptedException {
        final File tempImageTar = File.createTempFile("spark-job-image", ".tar");
        tempImageTar.deleteOnExit();

        // 1. Save the Testcontainers image from the host Docker daemon to a tarball
        try (InputStream imageStream = DockerClientFactory.instance().client().saveImageCmd(JOB_SPARK_IMAGE).exec();
                FileOutputStream fos = new FileOutputStream(tempImageTar)) {
            IOUtils.copy(imageStream, fos);
        }

        // 2. Transfer the tarball into the K3s cluster container
        K3S.copyFileToContainer(MountableFile.forHostPath(tempImageTar.toPath()), "/tmp/spark-job-image.tar");

        // 3. Import the tarball into the K3s internal registry (k8s.io namespace)
        final ExecResult execResult = K3S.execInContainer("ctr", "-n", "k8s.io", "images", "import",
                "/tmp/spark-job-image.tar");

        if (execResult.getExitCode() != 0) {
            throw new RuntimeException("Failed to import image into K3s: " + execResult.getStderr());
        }
    }
}