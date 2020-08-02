package io.datasearch.diseasedata.store.sourceconnector;

import feign.Feign;
import feign.Retryer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Source Client to send requests to API.
 */
public class SourceClient {

    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataSourceConnector.class);

    public static List<Event> getRecords(String baseUrl) {
        try {
            return Feign.builder()
                    .retryer(Retryer.NEVER_RETRY)
                    .target(SourceRequest.class, baseUrl)
                    .getRecords();
        } catch (Exception e) {
            logger.error("Caught following exception swallowing to ensure connector doesn't explode", e);
            return null;
        }
    }
}
