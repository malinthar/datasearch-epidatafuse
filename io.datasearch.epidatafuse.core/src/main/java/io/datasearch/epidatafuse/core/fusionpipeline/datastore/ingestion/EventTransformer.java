package io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.PipelineDataStore;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.AttributeUtil;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.siddhi.core.event.Event;
import org.geotools.data.DataStore;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.util.factory.Hints;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Transform Streaming Siddhi events to SimpleFeatures.
 */
public class EventTransformer {
    private static final Logger logger = LoggerFactory.getLogger(PipelineDataStore.class);
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";
    private static final String ATTRIBUTE_TYPE_KEY = "attribute_type";
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private MessageDigest md5;
    //todo: use string constants, handle schema mismatch

    public EventTransformer() {
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            logger.error("MD5 could not be instantiated!");
        }
    }

    public void transformAndWriteEvent(Event[] events,
                                       SimpleFeatureTypeSchema simpleFeatureTypeSchema,
                                       DataStore dataStore) {
        try {
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                    dataStore.getFeatureWriterAppend(simpleFeatureTypeSchema.getSimpleFeatureTypeName(),
                            Transaction.AUTO_COMMIT);

            List<Map<String, String>> attributes = simpleFeatureTypeSchema.getAttributes();
            List<Map<String, String>> records = getMappedRecords(events, attributes);
            for (Map<String, String> record : records) {
                SimpleFeature next = writer.next();
                next.getUserData().put(Hints.PROVIDED_FID, generateFeatureID(record.toString()));
                for (Map<String, String> attribute : simpleFeatureTypeSchema.getAttributes()) {
                    String attributeName = attribute.get(ATTRIBUTE_NAME_KEY);
                    String attributeType = attribute.get(ATTRIBUTE_TYPE_KEY);
                    String attributeValue = record.get(attributeName);
                    Object value = AttributeUtil.convert(attributeValue, attributeType);
                    next.setAttribute(attribute.get(ATTRIBUTE_NAME_KEY), value);
                }
                writer.write();
                logger.info("Added new record," + next.getID() + " to " +
                        simpleFeatureTypeSchema.getSimpleFeatureTypeName());
            }
        } catch (IOException e) {
            logger.error("Event transformation error");
        }
    }

    public List<Map<String, String>> getMappedRecords(Event[] events,
                                                      List<Map<String, String>> attributes) {
        List<Map<String, String>> records = new ArrayList<>();
        for (Event event : events) {
            int index = 0;    //modify to get the source config
            if (attributes.size() == event.getData().length) {
                Map<String, String> record = new HashMap<>();
                for (Map<String, String> attribute : attributes) {
                    record.put(attribute.get(ATTRIBUTE_NAME_KEY), (String) event.getData(index));
                    index++;
                }
                records.add(record);
            } else {
                logger.error("Event dropped: Schema and input mismatch");
            }
        }
        return records;
    }

    public String generateFeatureID(String record) {
        byte[] recordBytes = record.getBytes(DEFAULT_CHARSET);
        byte[] md5sum = this.md5.digest(recordBytes);
        String featureID = String.format("%032X", new BigInteger(1, md5sum));
        return featureID;
    }
}
