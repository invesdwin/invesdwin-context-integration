package de.invesdwin.context.integration.script;

import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import de.invesdwin.util.math.decimal.ADecimal;
import de.invesdwin.util.math.decimal.Decimal;

public interface IScriptTaskInputs {

    IScriptTaskEngine getEngine();

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

    void putFloat(String variable, float value);

    void putFloatVector(String variable, float[] value);

    default void putFloatVectorAsList(final String variable, final List<Float> value) {
        putFloatVector(variable, ArrayUtils.toPrimitive(value.toArray(new Float[value.size()])));
    }

    void putFloatMatrix(String variable, float[][] value);

    default void putFloatMatrixAsList(final String variable, final List<? extends List<Float>> value) {
        final float[][] matrix = new float[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<Float> vector = value.get(i);
            matrix[i] = ArrayUtils.toPrimitive(vector.toArray(new Float[vector.size()]));
        }
        putFloatMatrix(variable, matrix);
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

    default void putDecimal(final String variable, final ADecimal<?> value) {
        putDouble(variable, value.getDefaultValue().doubleValueRaw());
    }

    default <T extends ADecimal<?>> void putDecimalVector(final String variable, final T[] value) {
        putDoubleVector(variable, Decimal.toPrimitive(value));
    }

    default void putDecimalVectorAsList(final String variable, final List<? extends ADecimal<?>> value) {
        putDoubleVector(variable, Decimal.toPrimitive(value.toArray(new ADecimal[value.size()])));
    }

    default <T extends ADecimal<?>> void putDecimalMatrix(final String variable, final T[][] value) {
        final double[][] matrix = new double[value.length][];
        for (int i = 0; i < value.length; i++) {
            final T[] vector = value[i];
            matrix[i] = Decimal.toPrimitive(vector);
        }
        putDoubleMatrix(variable, matrix);
    }

    default void putDecimalMatrixAsList(final String variable,
            final List<? extends List<? extends ADecimal<?>>> value) {
        final double[][] matrix = new double[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<? extends ADecimal<?>> vector = value.get(i);
            matrix[i] = Decimal.toPrimitive(vector.toArray(new ADecimal[vector.size()]));
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

    void putLong(String variable, long value);

    void putLongVector(String variable, long[] value);

    default void putLongVectorAsList(final String variable, final List<Long> value) {
        putLongVector(variable, ArrayUtils.toPrimitive(value.toArray(new Long[value.size()])));
    }

    void putLongMatrix(String variable, long[][] value);

    default void putLongMatrixAsList(final String variable, final List<? extends List<Long>> value) {
        final long[][] matrix = new long[value.size()][];
        for (int i = 0; i < value.size(); i++) {
            final List<Long> vector = value.get(i);
            matrix[i] = ArrayUtils.toPrimitive(vector.toArray(new Long[vector.size()]));
        }
        putLongMatrix(variable, matrix);
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

}
