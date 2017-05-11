package de.invesdwin.context.integration.script;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

import de.invesdwin.util.math.decimal.ADecimal;
import de.invesdwin.util.math.decimal.Decimal;

public interface IScriptTaskResults {

    boolean isDefined(String variable);

    default boolean isNotDefined(final String variable) {
        return !isDefined(variable);
    }

    boolean isNull(String variable);

    default boolean isNotNull(final String variable) {
        return !isNull(variable);
    }

    IScriptTaskEngine getEngine();

    String getString(String variable);

    String[] getStringVector(String variable);

    default List<String> getStringVectorAsList(final String variable) {
        return Arrays.asList(getStringVector(variable));
    }

    String[][] getStringMatrix(String variable);

    default List<List<String>> getStringMatrixAsList(final String variable) {
        final String[][] matrix = getStringMatrix(variable);
        final List<List<String>> matrixAsList = new ArrayList<>(matrix.length);
        for (final String[] vector : matrix) {
            matrixAsList.add(Arrays.asList(vector));
        }
        return matrixAsList;
    }

    double getDouble(String variable);

    double[] getDoubleVector(String variable);

    default List<Double> getDoubleVectorAsList(final String variable) {
        return Arrays.asList(ArrayUtils.toObject(getDoubleVector(variable)));
    }

    double[][] getDoubleMatrix(String variable);

    default List<List<Double>> getDoubleMatrixAsList(final String variable) {
        final double[][] matrix = getDoubleMatrix(variable);
        final List<List<Double>> matrixAsList = new ArrayList<>(matrix.length);
        for (final double[] vector : matrix) {
            matrixAsList.add(Arrays.asList(ArrayUtils.toObject(vector)));
        }
        return matrixAsList;
    }

    default Decimal getDecimal(final String variable) {
        return getDecimal(variable, Decimal.ZERO);
    }

    default Decimal[] getDecimalVector(final String variable) {
        return getDecimalVector(variable, Decimal.ZERO);
    }

    default List<Decimal> getDecimalVectorAsList(final String variable) {
        return getDecimalVectorAsList(variable, Decimal.ZERO);
    }

    default Decimal[][] getDecimalMatrix(final String variable) {
        return getDecimalMatrix(variable, Decimal.ZERO);
    }

    default List<List<Decimal>> getDecimalMatrixAsList(final String variable) {
        return getDecimalMatrixAsList(variable, Decimal.ZERO);
    }

    default <T extends ADecimal<T>> T getDecimal(final String variable, final T converter) {
        return converter.fromDefaultValue(new Decimal(getDouble(variable)));
    }

    default <T extends ADecimal<T>> T[] getDecimalVector(final String variable, final T converter) {
        return converter.fromDefaultValue(Decimal.toObject(getDoubleVector(variable)));
    }

    default <T extends ADecimal<T>> List<T> getDecimalVectorAsList(final String variable, final T converter) {
        return Arrays.asList(getDecimalVector(variable, converter));
    }

    @SuppressWarnings("unchecked")
    default <T extends ADecimal<T>> T[][] getDecimalMatrix(final String variable, final T converter) {
        final double[][] matrix = getDoubleMatrix(variable);
        final T[][] matrixAsList = (T[][]) Array.newInstance(converter.getClass(), matrix.length, matrix[0].length);
        for (int i = 0; i < matrix.length; i++) {
            converter.fromDefaultValue(Decimal.toObject(matrix[i]), matrixAsList[i]);
        }
        return matrixAsList;
    }

    default <T extends ADecimal<T>> List<List<T>> getDecimalMatrixAsList(final String variable, final T converter) {
        final T[][] matrix = getDecimalMatrix(variable, converter);
        final List<List<T>> matrixAsList = new ArrayList<>(matrix.length);
        for (final T[] vector : matrix) {
            matrixAsList.add(Arrays.asList(vector));
        }
        return matrixAsList;
    }

    int getInteger(String variable);

    int[] getIntegerVector(String variable);

    default List<Integer> getIntegerVectorAsList(final String variable) {
        return Arrays.asList(ArrayUtils.toObject(getIntegerVector(variable)));
    }

    int[][] getIntegerMatrix(String variable);

    default List<List<Integer>> getIntegerMatrixAsList(final String variable) {
        final int[][] matrix = getIntegerMatrix(variable);
        final List<List<Integer>> matrixAsList = new ArrayList<>(matrix.length);
        for (final int[] vector : matrix) {
            matrixAsList.add(Arrays.asList(ArrayUtils.toObject(vector)));
        }
        return matrixAsList;
    }

    boolean getBoolean(String variable);

    boolean[] getBooleanVector(String variable);

    default List<Boolean> getBooleanVectorAsList(final String variable) {
        return Arrays.asList(ArrayUtils.toObject(getBooleanVector(variable)));
    }

    boolean[][] getBooleanMatrix(String variable);

    default List<List<Boolean>> getBooleanMatrixAsList(final String variable) {
        final boolean[][] matrix = getBooleanMatrix(variable);
        final List<List<Boolean>> matrixAsList = new ArrayList<>(matrix.length);
        for (final boolean[] vector : matrix) {
            matrixAsList.add(Arrays.asList(ArrayUtils.toObject(vector)));
        }
        return matrixAsList;
    }

}
