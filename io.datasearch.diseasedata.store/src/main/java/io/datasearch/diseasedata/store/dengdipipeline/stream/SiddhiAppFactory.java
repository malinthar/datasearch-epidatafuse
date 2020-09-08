package io.datasearch.diseasedata.store.dengdipipeline.stream;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SiddhiAppBuilder.
 */
public class SiddhiAppFactory {
    private static final Logger logger = LoggerFactory.getLogger(SiddhiAppFactory.class);
    private static final SiddhiManager siddhiManager = new SiddhiManager();


    public static SiddhiAppRuntime generateTemplate() throws InterruptedException {
        try {
            String siddhiApp = "" +
                    "@sink(type='http-call', sink.id='production-request', " +
                    "publisher.url='http://127.0.0.1:5000/sweet'," +
                    "method='GET', @map(type='json'))" +
                    "" +
                    "define stream CallStream (name string, amount double);" +
                    "" +
                    "@source(type='http-call-response' , sink.id='production-request', " +
                    "http.status.code='200'," +
                    "@map(type='json'))" +
                    "" +
                    "define stream ResponseStream(name string, temperature double);";

            //Generate runtime
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp);
            return siddhiAppRuntime;

        } catch (Exception e) {
            throw e;
        }
    }
}

