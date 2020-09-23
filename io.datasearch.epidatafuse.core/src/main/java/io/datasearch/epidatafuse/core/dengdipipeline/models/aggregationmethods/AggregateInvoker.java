package io.datasearch.epidatafuse.core.dengdipipeline.models.aggregationmethods;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

/**
 * aggregation methods
 */
public class AggregateInvoker {
    private static final Logger logger = LoggerFactory.getLogger(AggregateInvoker.class);

    public static Double mean(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, "mean");
        return finalValue;
    }

    public static Double sum(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, "sum");
        return finalValue;
    }

    public static Double max(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, "max");
        return finalValue;
    }

    public static Double min(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, "min");
        return finalValue;
    }

    public static Double inverseDistance(HashMap<String, Double> valueSet, HashMap<String, Double> distances) {
        Double finalValue = 0.0;
        return finalValue;
    }

    public static double calculate(ArrayList<Double> values, String method) {
        Double calculatedValue = 0.0;
        switch (method) {
            case "mean":
                Double sum = 0.0;
                int count = 0;
                for (Double value : values) {
                    try {
                        sum = sum + value;
                        count = count + 1;
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }
                }
                Double mean = sum / count;
                calculatedValue = mean;
                break;

            case "sum":
                sum = 0.0;
                for (Double value : values) {
                    try {
                        sum = sum + value;
                    } catch (Exception e) {
                        logger.info(e.getMessage());
                    }
                }
                calculatedValue = sum;
                break;

            case "max":
                calculatedValue = Collections.max(values);
                break;

            case "min":
                calculatedValue = Collections.min(values);
                break;
            default:
                calculatedValue = 0.0;
                break;
        }

        return calculatedValue;
    }
}
