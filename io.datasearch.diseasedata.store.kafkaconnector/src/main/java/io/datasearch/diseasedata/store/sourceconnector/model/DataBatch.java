package io.datasearch.diseasedata.store.sourceconnector.model;

import java.util.List;

/**
 * Collection of Data points.
 */
public class DataBatch {
    private List<DataPoint> dataPoints;
    public DataBatch(List<DataPoint> dataPointslist) {
        dataPoints = dataPointslist;
    }
    public List<DataPoint> getDataPoints() {
        return this.dataPoints;
    }
}
