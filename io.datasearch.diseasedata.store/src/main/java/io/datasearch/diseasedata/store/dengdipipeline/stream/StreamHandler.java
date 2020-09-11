package io.datasearch.diseasedata.store.dengdipipeline.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To handle streams of data sources.
 */
public class StreamHandler {
    private DengDIKafkaConsumer kafkaConsumer;
    private static final Logger logger = LoggerFactory.getLogger(StreamHandler.class);

    public StreamHandler() {
        createKafkaConsumer();
        try {
            this.kafkaConsumer.initConsumer();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void createKafkaConsumer() {
        this.kafkaConsumer = new DengDIKafkaConsumer();
    }
}
