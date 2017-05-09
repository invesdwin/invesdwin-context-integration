package de.invesdwin.context.integration.script;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

public interface IScriptTaskResults extends Closeable {

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

    @Override
    void close();

    Object getEngine();

}
