package de.invesdwin.context.integration.hadoop.mapreduce;

import java.io.IOException;

import javax.annotation.concurrent.NotThreadSafe;

import org.apache.hadoop.io.Text;

import com.google.common.collect.Iterables;

@NotThreadSafe
public class HadoopTestJobReducer extends AReducer<Text, Text, Text, Text> {

    public HadoopTestJobReducer() {
        super(false);
    }

    @Override
    protected void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
        final String valuesStr = Iterables.toString(values);
        context.write(new Text(key + "_reduced"), new Text(valuesStr + "_reduced"));
    }

}
