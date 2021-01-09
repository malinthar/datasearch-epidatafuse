package io.datasearch.epidatafuse.core.fusionpipeline.stream;

import io.datasearch.epidatafuse.core.fusionpipeline.FusionPipeline;
import io.datasearch.epidatafuse.core.fusionpipeline.model.granularitymappingmethod.TemporalRelationship;
import io.siddhi.core.SiddhiAppRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * To handle streams of data sources.
 */
public class StreamHandler {
    private Map<String, SiddhiAppRuntime> sourceConnections;
    private FusionPipeline pipeline;
    private static final Logger logger = LoggerFactory.getLogger(StreamHandler.class);
    private static final String REQUEST_FREQUENCY_KEY = "request_frequency";
    private static final String GRANULARITY_KEY = "granularity";
    private static final String MULTIPLIER_KEY = "multiplier";
    private static final String URL_KEY = "url";

    //modify to get the source config
    public StreamHandler(FusionPipeline pipeline) {
        try {
            this.pipeline = pipeline;
            this.sourceConnections = new HashMap<>();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void init() {
        startSourceConnections();
    }

    public void addStreamingConfiguration(String featureName, Map<String, Object> parameters) {
        Map<String, Object> requestFQ = (HashMap<String, Object>) parameters.get(REQUEST_FREQUENCY_KEY);
        String granularity = (String) requestFQ.get(GRANULARITY_KEY);
        String url = (String) parameters.get(URL_KEY);
        int multiplier = (int) requestFQ.get(MULTIPLIER_KEY);
        long requestFrequency =
                TemporalRelationship.getGranularityToSeconds(granularity) * multiplier;
        sourceConnections.put(featureName, SiddhiAppFactory.
                generateSourceConnection(url, requestFrequency, pipeline.getSchema(featureName)));
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
