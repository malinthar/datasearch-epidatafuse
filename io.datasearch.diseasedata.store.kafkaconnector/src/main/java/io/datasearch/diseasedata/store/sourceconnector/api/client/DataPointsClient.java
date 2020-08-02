package io.datasearch.diseasedata.store.sourceconnector.api.client;

import com.google.gson.GsonBuilder;
import feign.Feign;
import feign.Retryer;
import feign.gson.GsonDecoder;
import feign.slf4j.Slf4jLogger;
import io.datasearch.diseasedata.store.sourceconnector.DiseaseDataSourceConnector;
import io.datasearch.diseasedata.store.sourceconnector.api.DiseaseDataPoint;
import io.datasearch.diseasedata.store.sourceconnector.model.DataBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Source Client to send requests to API.
 */
public class DataPointsClient {

    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataSourceConnector.class);

    public static DataBatch getRecords(String baseUrl) {
        try {
            return Feign.builder()
                    .decoder(dataBatchDecoder())
                    .logger(new Slf4jLogger())
                    .retryer(Retryer.NEVER_RETRY)
                    .target(DiseaseDataPoint.class, baseUrl)
                    .getRecords();
        } catch (Exception e) {
            logger.error("Caught following exception swallowing to ensure connector doesn't explode", e);
            return null;
        }
    }

    private static GsonDecoder dataBatchDecoder() {
        return new GsonDecoder(
                new GsonBuilder().create());
    }
}
