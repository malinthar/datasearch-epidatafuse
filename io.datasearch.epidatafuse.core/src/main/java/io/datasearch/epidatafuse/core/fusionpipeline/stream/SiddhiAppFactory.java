package io.datasearch.epidatafuse.core.fusionpipeline.stream;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * SiddhiAppBuilder.
 */
public class SiddhiAppFactory {
    private static final Logger logger = LoggerFactory.getLogger(SiddhiAppFactory.class);
    private static final SiddhiManager siddhiManager = new SiddhiManager();
    private static final String URL_KEY = "url";
    private static final String REQUEST_FREQUENCY = "request_frequency";
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";

    public static SiddhiAppRuntime generateSourceConnection(String url, long requestFrequency,
                                                            SimpleFeatureTypeSchema schema) {

        StringBuilder siddhiApp = new StringBuilder("");
        siddhiApp.append(generateHttpCallResponse(url, schema));
        siddhiApp.append("define trigger TimeTriggerStream at every " +
                5 + " sec;");
        siddhiApp.append(
                "@info(name= 'pass_though') " +
                        "from TimeTriggerStream " +
                        "select eventTimestamp() as timestamp " +
                        "insert into CallStream;");
        try {
            SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(siddhiApp.toString());
            return siddhiAppRuntime;
        } catch (Exception e) {
            throw e;
        }
    }

    public static String generateHttpCallResponse(String url, SimpleFeatureTypeSchema schema) {
        StringBuilder httpCallResponse = new StringBuilder();
        httpCallResponse.append("@sink(type='http-call', sink.id='" + schema.getSimpleFeatureTypeName() + "',");
        httpCallResponse.append("publisher.url='" + url + "',");
        httpCallResponse.append("method='POST', @map(type='json'))");
        httpCallResponse.append("define stream CallStream (timestamp long);");
        httpCallResponse.append("@source(type='http-call-response' , sink.id='" +
                schema.getSimpleFeatureTypeName() + "', " +
                "http.status.code='200'," +
                "@map(type='json'))");
        httpCallResponse.append("define stream ResponseStream(");
        List<Map<String, String>> attributes = schema.getAttributes();
        for (Map<String, String> attribute : attributes) {
            if (attributes.indexOf(attribute) == 0) {
                httpCallResponse.append(attribute.get(ATTRIBUTE_NAME_KEY) + " string");
            } else {
                httpCallResponse.append(", " + attribute.get(ATTRIBUTE_NAME_KEY) + " string");
            }
        }
        httpCallResponse.append(");");
        return httpCallResponse.toString();
    }
}

