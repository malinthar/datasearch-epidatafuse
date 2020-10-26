package io.datasearch.epidatafuse.core.fusionpipeline.stream;

import io.datasearch.epidatafuse.core.fusionpipeline.FusionPipeline;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;

/**
 * SourceConnector class for receiving events.
 */
public class SourceConnector extends StreamCallback {

    private FusionPipeline pipeline;
    private String featureType;

    public SourceConnector(FusionPipeline pipeline, String featureType) {
        this.pipeline = pipeline;
        this.featureType = featureType;
    }

    @Override
    public void receive(Event[] events) {
        //To convert and print event as a map
        //EventPrinter.print(toMap(events));
        pipeline.streamingIngest(events, this.featureType);
    }
}
