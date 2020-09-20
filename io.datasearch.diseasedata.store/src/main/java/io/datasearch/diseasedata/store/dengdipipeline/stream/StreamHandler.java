package io.datasearch.diseasedata.store.dengdipipeline.stream;

import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLine;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.event.Event;
import io.siddhi.core.stream.input.InputHandler;
import io.siddhi.core.stream.output.StreamCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * To handle streams of data sources.
 */
public class StreamHandler extends StreamCallback {
    private SiddhiAppRuntime sourceConnection;
    private DengDIPipeLine pipeline;
    private static final Logger logger = LoggerFactory.getLogger(StreamHandler.class);

    //modify to get the source config
    public StreamHandler(DengDIPipeLine pipeline) {
        try {
            this.pipeline = pipeline;
            this.sourceConnection = SiddhiAppFactory.generateTemplate();
            startSourceConnection();
            Thread.sleep(10000);
            terminateSourceConnection();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
    }

    public void startSourceConnection() {
        if (sourceConnection != null) {
            try {
                //Adding callback to retrieve output events from stream
                sourceConnection.addCallback("ResponseStream", this);
                sourceConnection.start();
                InputHandler inputHandler = this.sourceConnection.getInputHandler("CallStream");
                //Start processing

                //Sending events to Siddhi
                inputHandler.send(new Object[]{"WSO2", 55.5});
                inputHandler.send(new Object[]{"GOOG", 60.1});
                inputHandler.send(new Object[]{"IBM", 78.5});
                inputHandler.send(new Object[]{"WSO2", 89.5});
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    @Override
    public void receive(Event[] events) {
        //To convert and print event as a map
        //EventPrinter.print(toMap(events));
        pipeline.streamingIngest(events);
    }

    public void terminateSourceConnection() {
        if (sourceConnection != null) {
            sourceConnection.shutdown();
        }
    }

}
