package de.invesdwin.common.integration.hadoop.mapreduce;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.type.filter.AssignableTypeFilter;
import org.springframework.util.ClassUtils;

import de.invesdwin.context.beans.init.MergedContext;
import de.invesdwin.context.beans.init.PreMergedContext;
import de.invesdwin.context.log.error.Err;
import de.invesdwin.context.system.classpath.ClasspathResourceProcessor;
import de.invesdwin.context.system.classpath.IClasspathResourceVisitor;
import de.invesdwin.instrument.DynamicInstrumentationReflections;
import de.invesdwin.util.assertions.Assertions;
import de.invesdwin.util.collections.loadingcache.ALoadingCache;
import de.invesdwin.util.lang.Reflections;
import hadoop.test.job.mapper.isolated.HadoopTestJobMapperIsolatedBean;
import hadoop.test.job.mapper.isolated.scanned.HadoopTestJobMapperIsolatedScannedBean;

@NotThreadSafe
public class HadoopTestJobMapper extends AMapper<LongWritable, Text, Text, Text> {

    private boolean firstRun = true;

    @Inject
    private HadoopTestJobMapperBean bean;

    public HadoopTestJobMapper() {
        super(false);
    }

    @Override
    protected void map(final LongWritable key, final Text value, final Context context) throws IOException,
    InterruptedException {
        if (firstRun) {
            firstRun = false;
            checkDuplicateClasses();
            checkGetResource();
            checkClasspathScanning();
            checkIsolatedSpringContext();
            checkMergedContext();
        } else {
            Assertions.assertThat(bean).isNotNull();
        }
        context.write(new Text(key + "_mapped"), new Text(value + "_mapped"));
    }

    private void checkDuplicateClasses() {
        new ClasspathResourceProcessor().process(new IClasspathResourceVisitor() {
            private final ALoadingCache<String, Set<String>> resource_fullPaths = new ALoadingCache<String, Set<String>>() {
                @Override
                protected Set<String> loadValue(final String key) {
                    return new HashSet<String>();
                }
            };

            @Override
            public boolean visit(final String fullPath, final String resourcePath, final InputStream inputStream) {
                for (final String pathAdded : DynamicInstrumentationReflections.getPathsAddedToSystemClassLoader()) {
                    if (fullPath.startsWith(pathAdded)) {
                        return true;
                    }
                }
                resource_fullPaths.get(resourcePath).add(fullPath);
                return true;
            }

            @Override
            public void finish() {
                final StringBuilder sb = new StringBuilder();
                int duplicates = 0;
                for (final Entry<String, Set<String>> e : resource_fullPaths.entrySet()) {
                    final String resource = e.getKey();
                    if (resource.endsWith(".class")) {
                        final Set<String> fullPaths = e.getValue();
                        if (fullPaths.size() > 1) {
                            boolean fromJobJar = false;
                            for (final String fullPath : fullPaths) {
                                if (fullPath.contains("job.jar!")) {
                                    fromJobJar = true;
                                    break;
                                }
                            }
                            if (fromJobJar) {
                                duplicates++;
                                sb.append("\nDuplicate [" + resource + "] in: " + fullPaths);
                            }
                        }
                    }
                }
                Assertions.assertThat(duplicates)
                .as("Found %s duplicate classes introduced by job: %s", duplicates, sb)
                .isZero();
            }
        });
    }

    private void checkGetResource() throws IOException {
        final Resource resource = PreMergedContext.getInstance().getResource(
                "classpath:/" + HadoopTestJobMapperIsolatedScannedBean.class.getName().replace(".", "/") + ".class");
        Assertions.assertThat(resource.contentLength()).isGreaterThan(0);

        final Resource[] resourcesAsterisk = PreMergedContext.getInstance().getResources(
                "classpath*:/" + HadoopTestJobMapperIsolatedScannedBean.class.getName().replace(".", "/") + ".class");
        Assertions.assertThat(resourcesAsterisk.length).isEqualTo(1);
    }

    private void checkClasspathScanning() {
        final org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider provider = new org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider(
                false);
        provider.addIncludeFilter(new AssignableTypeFilter(HadoopTestJobMapperIsolatedScannedBean.class));
        final Set<BeanDefinition> findCandidateComponents = provider.findCandidateComponents(HadoopTestJobMapperIsolatedScannedBean.class.getPackage()
                .getName());
        Assertions.assertThat(findCandidateComponents.size()).as(findCandidateComponents.toString()).isEqualTo(1);
    }

    private void checkIsolatedSpringContext() {
        final ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
                "classpath:/META-INF/ctx.hadoop.test.job.mapper.isolated.xml");
        final HadoopTestJobMapperIsolatedBean declaredBean = ctx.getBean(HadoopTestJobMapperIsolatedBean.class);
        Assertions.assertThat(declaredBean).isNotNull();
        Assertions.assertThat(declaredBean.test()).isTrue();

        Assertions.assertThat(Reflections.classExists(HadoopTestJobMapperIsolatedScannedBean.class.getName()))
        .isNotNull();
        try {
            Assertions.assertThat(
                    Class.forName(HadoopTestJobMapperIsolatedScannedBean.class.getName(), true,
                            ClassUtils.getDefaultClassLoader())).isNotNull();
        } catch (final ClassNotFoundException e) {
            throw Err.process(e);
        }

        final HadoopTestJobMapperIsolatedScannedBean scannedBean = ctx.getBean(HadoopTestJobMapperIsolatedScannedBean.class);
        Assertions.assertThat(scannedBean).isNotNull();
        Assertions.assertThat(scannedBean.test()).isTrue();
    }

    private void checkMergedContext() {
        new LoadTimeWeavingTest().testNotWeaved();
        Assertions.assertThat(bean).isNull();
        MergedContext.autowire(this);
        Assertions.assertThat(bean).isNotNull();
        Assertions.assertThat(bean.test()).isTrue();
        new LoadTimeWeavingTest().testWeaved();
    }

    @Configurable
    private static class LoadTimeWeavingTest {

        @Inject
        private HadoopTestJobMapperBean ltwBean;

        public void testNotWeaved() {
            Assertions.assertThat(ltwBean).isNull();
        }

        public void testWeaved() {
            Assertions.assertThat(ltwBean).isNotNull();
            Assertions.assertThat(ltwBean.test()).isTrue();
        }

    }

}
