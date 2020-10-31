package io.datasearch.epidatafuse.core.dengdipipeline.stream;

import io.datasearch.epidatafuse.core.dengdipipeline.DengDIPipeLine;
import io.datasearch.epidatafuse.core.util.ConfigurationLoader;
import io.siddhi.core.SiddhiAppRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * To handle streams of data sources.
 */
public class StreamHandler {
    private Map<String, SiddhiAppRuntime> sourceConnections;
    private DengDIPipeLine pipeline;
    private static final Logger logger = LoggerFactory.getLogger(StreamHandler.class);
    private static final String SOURCES_KEY = "sources"; //todo:add to a common class

    //modify to get the source config
    public StreamHandler(DengDIPipeLine pipeline) {
        try {
            this.pipeline = pipeline;
            generateSourceConnections();
            startSourceConnections();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void generateSourceConnections() {
        this.sourceConnections = new HashMap<>();
        List<Map<String, Object>> sources = (List<Map<String, Object>>) ConfigurationLoader.
                getIngestStreamingConfigurations().get(SOURCES_KEY);
        for (Map<String, Object> source : sources) {
            sourceConnections.put((String) source.get("feature_name"), SiddhiAppFactory.
                    generateSourceConnection(source,
                            pipeline.getSchema((String) source.get("feature_name"))));
        }
    }

    public void startSourceConnections() {
        if (sourceConnections != null) {
            try {
                for (Map.Entry<String, SiddhiAppRuntime> sourceConnection : sourceConnections.entrySet()) {
                    sourceConnection.getValue().addCallback("ResponseStream",
                            new SourceConnector(this.pipeline, sourceConnection.getKey()));
                    sourceConnection.getValue().start();
                    //Adding callback to retrieve output events from stream
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
            }
        }
    }

    public void terminateSourceConnections() {
        if (sourceConnections != null) {
            sourceConnections.values().forEach(sourceConnection -> {
                sourceConnection.shutdown();
            });
        }
    }

}
