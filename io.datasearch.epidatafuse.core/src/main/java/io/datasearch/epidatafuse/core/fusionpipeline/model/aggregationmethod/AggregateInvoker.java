package io.datasearch.epidatafuse.core.fusionpipeline.model.aggregationmethod;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * aggregation methods
 */
public class AggregateInvoker {
    private static final Logger logger = LoggerFactory.getLogger(AggregateInvoker.class);
    public static Double mean(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, AggregationUtil.MEAN);
        return finalValue;
    }

    public static Double sum(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, AggregationUtil.SUM);
        return finalValue;
    }

    public static Double max(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, AggregationUtil.MAX);
        return finalValue;
    }

    public static Double min(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        finalValue = calculate(values, AggregationUtil.MIN);
        return finalValue;
    }

    public static Double inverseDistance(HashMap<String, Double> valueSet, HashMap<String, Double> distances) {
        Double finalValue = 0.0;
        Double inverseDistanceSum = 0.0;

        for (Double distance : distances.values()) {
            if (distance != 0) {
                inverseDistanceSum += (1.0 / distance);
            }
        }

        Iterator it = valueSet.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            String key = (String) pair.getKey();
            Double value = (Double) pair.getValue();

            Double distance = distances.get(key);

            if (distance != 0) {
                Double weight = ((1.0 / distance) / inverseDistanceSum);

                finalValue += value * weight;
            }
        }

        return finalValue;
    }

    public static Double defaultAggregate(HashMap<String, Double> valueSet) {
        Double finalValue;
        ArrayList<Double> values = new ArrayList<Double>(valueSet.values());
        if (values.size() >= 1) {
            finalValue = values.get(0);
        } else {
            finalValue = 0.0;
        }
        return finalValue;
    }

    public static double calculate(ArrayList<Double> values, String method) {
        Double calculatedValue = 0.0;
        switch (method) {
            case AggregationUtil.MEAN:
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

            case AggregationUtil.SUM:
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

            case AggregationUtil.MAX:
                calculatedValue = Collections.max(values);
                break;

            case AggregationUtil.MIN:
                calculatedValue = Collections.min(values);
                break;
            default:
                calculatedValue = 0.0;
                break;
        }

        return calculatedValue;
    }
}
