package de.invesdwin.context.integration.script;

import java.io.Closeable;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

public interface IScriptTaskInputs extends Closeable {

    void putString(String variable, String value);

    void putStringVector(String variable, String[] value);

    default void putStringVectorAsList(final String variable, final List<String> value) {
        putStringVector(variable, value.toArray(new String[value.size()]));
    }

    void putStringMatrix(String variable, String[][] value);

    default void putStringMatrixAsList(final String variable, final List<? extends List<String>> value) {
        final String[][] matrix = new String[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<String> vector = value.get(i);
            matrix[i] = vector.toArray(new String[vector.size()]);
        }
        putStringMatrix(variable, matrix);
    }

    void putDouble(String variable, double value);

    void putDoubleVector(String variable, double[] value);

    default void putDoubleVectorAsList(final String variable, final List<Double> value) {
        putDoubleVector(variable, ArrayUtils.toPrimitive(value.toArray(new Double[value.size()])));
    }

    void putDoubleMatrix(String variable, double[][] value);

    default void putDoubleMatrixAsList(final String variable, final List<? extends List<Double>> value) {
        final double[][] matrix = new double[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<Double> vector = value.get(i);
            matrix[i] = ArrayUtils.toPrimitive(vector.toArray(new Double[vector.size()]));
        }
        putDoubleMatrix(variable, matrix);
    }

    void putInteger(String variable, int value);

    void putIntegerVector(String variable, int[] value);

    default void putIntegerVectorAsList(final String variable, final List<Integer> value) {
        putIntegerVector(variable, ArrayUtils.toPrimitive(value.toArray(new Integer[value.size()])));
    }

    void putIntegerMatrix(String variable, int[][] value);

    default void putIntegerMatrixAsList(final String variable, final List<? extends List<Integer>> value) {
        final int[][] matrix = new int[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<Integer> vector = value.get(i);
            matrix[i] = ArrayUtils.toPrimitive(vector.toArray(new Integer[vector.size()]));
        }
        putIntegerMatrix(variable, matrix);
    }

    void putBoolean(String variable, boolean value);

    void putBooleanVector(String variable, boolean[] value);

    default void putBooleanVectorAsList(final String variable, final List<Boolean> value) {
        putBooleanVector(variable, ArrayUtils.toPrimitive(value.toArray(new Boolean[value.size()])));
    }

    void putBooleanMatrix(String variable, boolean[][] value);

    default void putBooleanMatrixAsList(final String variable, final List<? extends List<Boolean>> value) {
        final boolean[][] matrix = new boolean[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<Boolean> vector = value.get(i);
            matrix[i] = ArrayUtils.toPrimitive(vector.toArray(new Boolean[vector.size()]));
        }
        putBooleanMatrix(variable, matrix);
    }

    void putExpression(String variable, String expression);

    Object getEngine();

    @Override
    void close();

}
