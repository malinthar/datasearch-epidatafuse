package io.datasearch.diseasedata.store.dengdipipeline;

import io.datasearch.diseasedata.store.dengdipipeline.aggregation.Aggregator;
import io.datasearch.diseasedata.store.dengdipipeline.publish.Publisher;
import io.datasearch.diseasedata.store.dengdipipeline.stream.StreamHandler;
import io.datasearch.diseasedata.store.schema.SchemaBuilder;
import org.locationtech.geomesa.hbase.data.HBaseDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DengDIPipeline is the processing unit for streaming and historical data.
 * PipeLine starts with streaming component which retrieves streaming data and ends with data publisher which publishes
 * aggregated data into specified endpoints.
 */
public class DengDIPipeLine {
    private static final Logger logger = LoggerFactory.getLogger(DengDIPipeLine.class);
    //Data store for persisting spatio-temporal data.
    private HBaseDataStore dataStore;
    //Stream handler for handling data
    private StreamHandler streamHandler;
    //aggregator and transformer;
    private Aggregator aggregator;
    //publisher for publishing data to relevant endpoints
    private Publisher publisher;

    public DengDIPipeLine(String[] params) {
        try {
            String myvar ="vjfnvgj";
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }
}
