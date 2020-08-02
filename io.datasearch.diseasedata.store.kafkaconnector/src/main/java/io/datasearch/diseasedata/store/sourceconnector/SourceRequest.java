package io.datasearch.diseasedata.store.sourceconnector;

import feign.Headers;
import feign.Param;
import feign.RequestLine;

import java.util.List;

/**
 * Source Request.
 */
public interface SourceRequest {
    @RequestLine("GET /test")
    @Headers({"Content-Type: application/json"})
    List<Event> getRecords(@Param("timestamp") String time);

    default List<Event> getRecords() {
        return getRecords("first");
    }

}
