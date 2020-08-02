package io.datasearch.diseasedata.store.sourceconnector.api;

import feign.Headers;
import feign.RequestLine;
import io.datasearch.diseasedata.store.sourceconnector.model.DataBatch;

/**
 * Source Request.
 */
public interface DiseaseDataPoint {

    @RequestLine("GET /test")
    @Headers({"Content-Type: application/json"})
    DataBatch getRecord();

    default DataBatch getRecords() {
        return getRecord();
    }
}
