package io.datasearch.epidatafuse.core.dengdipipeline.stream;

import io.datasearch.epidatafuse.core.dengdipipeline.DengDIPipeLine;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.output.StreamCallback;

/**
 * SourceConnector class for receiving events.
 */
public class SourceConnector extends StreamCallback {

    private DengDIPipeLine pipeline;
    private String featureType;

    public SourceConnector(DengDIPipeLine pipeline, String featureType) {
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
