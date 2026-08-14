package de.invesdwin.context.integration.grid.hadoop.filechannel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;

import org.apache.hadoop.conf.Configuration;

import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactory;
import de.invesdwin.context.integration.filechannel.registry.IFileChannelFactoryProvider;
import de.invesdwin.context.log.Log;
import de.invesdwin.util.collections.Collections;
import de.invesdwin.util.collections.factory.ILockCollectionFactory;

@Immutable
public class HadoopFileChannelFactoryProvider implements IFileChannelFactoryProvider {

    private static final Log LOG = new Log(HadoopFileChannelFactoryProvider.class);

    private static final Pattern FS_IMPL_PATTERN = Pattern.compile("^fs\\.([^.]+)\\.impl$");
    private static final Pattern ABSTRACT_FS_PATTERN = Pattern.compile("^fs\\.AbstractFileSystem\\.([^.]+)\\.impl$");
    private static final Pattern VIEWFS_OVERLOAD_PATTERN = Pattern
            .compile("^fs\\.viewfs\\.overload\\.scheme\\.target\\.([^.]+)\\.impl$");

    @Override
    public int getPriority() {
        return 20_000;
    }

    @Override
    public Collection<IFileChannelFactory> newFactories() {
        final List<IFileChannelFactory> factories = new ArrayList<>();
        try {
            final Configuration conf = new Configuration();
            final Iterator<Map.Entry<String, String>> it = conf.iterator();
            while (it.hasNext()) {
                final Map.Entry<String, String> entry = it.next();
                final String key = entry.getKey();

                if (key != null && key.endsWith(".impl")) {
                    String scheme = null;

                    final Matcher fsMatcher = FS_IMPL_PATTERN.matcher(key);
                    if (fsMatcher.matches()) {
                        scheme = fsMatcher.group(1).toLowerCase();
                    } else {
                        final Matcher absMatcher = ABSTRACT_FS_PATTERN.matcher(key);
                        if (absMatcher.matches()) {
                            scheme = absMatcher.group(1).toLowerCase();
                        } else {
                            final Matcher viewMatcher = VIEWFS_OVERLOAD_PATTERN.matcher(key);
                            if (viewMatcher.matches()) {
                                scheme = viewMatcher.group(1).toLowerCase();
                            }
                        }
                    }

                    if (scheme != null) {
                        factories.add(new HadoopFileChannelFactory(scheme));
                    }
                }
            }
        } catch (final Throwable t) {
            LOG.warn("Failed to query configured Hadoop FileSystem schemes", t);
        }

        final Map<String, IFileChannelFactory> uniqueFactories = ILockCollectionFactory.getInstance(false)
                .newLinkedMap();
        for (final IFileChannelFactory factory : factories) {
            uniqueFactories.putIfAbsent(factory.getScheme(), factory);
        }
        return Collections.unmodifiableCollection(uniqueFactories.values());
    }
}