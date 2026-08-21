package de.invesdwin.context.integration.grid.spark.test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import javax.annotation.concurrent.NotThreadSafe;

import org.testcontainers.k3s.K3sContainer;
import org.testcontainers.utility.DockerImageName;

import de.invesdwin.context.ContextProperties;
import de.invesdwin.util.lang.Files;

@NotThreadSafe
public class KubernetesContainer extends K3sContainer {

    private static final DockerImageName K3S_IMAGE = DockerImageName.parse("rancher/k3s:latest");
    //this file can be imported in k8s lens IDE
    private static final File KUBE_CONFIG_FILE = new File(ContextProperties.getUserHomeDirectory(),
            ".kube/" + KubernetesContainer.class.getSimpleName() + "_k3s-kubeconfig.yaml");

    private boolean initialized = false;

    public KubernetesContainer() {
        this(K3S_IMAGE);
    }

    public KubernetesContainer(final DockerImageName image) {
        super(image);
        initialized = true;
        //use fixed ports so that k8s lens IDE does not require reimporting the kubeconfig file on every test run
        addFixedExposedPort(KUBE_SECURE_PORT, KUBE_SECURE_PORT);
        addFixedExposedPort(RANCHER_WEBHOOK_PORT, RANCHER_WEBHOOK_PORT);
    }

    @Override
    public void addExposedPort(final Integer port) {
        if (!initialized) {
            //ignore in super constructor, instead add fixed ports
            return;
        }
        super.addExposedPort(port);
    }

    @Override
    public void start() {
        super.start();
        final String kubeConfigYaml = getKubeConfigYaml();
        try {
            Files.forceMkdirParent(KUBE_CONFIG_FILE);
            Files.writeStringToFile(KUBE_CONFIG_FILE, kubeConfigYaml, Charset.defaultCharset());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public File getKubeConfigFile() {
        return KUBE_CONFIG_FILE;
    }

}