package io.datasearch.diseasedata.store.sourceconnector;

import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Source Task.
 */
public class DiseaseDataSourceTask extends SourceTask {
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataSourceConnector.class);

    public static final String URL = "url";
    public static final String LAST_READ = "last_read";
    public static final String POLL_INTERVAL = "interval";
    public static final String LAST_API_OFFSET = "last_api_offset";
    public static final String DEFAULT_FROM_TIME = "1984-05-04T00:00:00.0000000Z";
    private String fromDate = DEFAULT_FROM_TIME;
    private String baseUrl;
    private String topic;
    private long apiOffset;
    private Long lastExecution = 0L;
    private Long interval;

    static boolean isNotNullOrBlank(String str) {
        return str != null && !str.trim().isEmpty();
    }

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        setupTaskConfig(props);
        logger.debug("Trying to get persistedMap.");
        Map<String, Object> persistedMap = null;
        if (context != null && context.offsetStorageReader() != null) {
            persistedMap = context.offsetStorageReader().offset(Collections.singletonMap(URL, baseUrl));
        }
        logger.info("The persistedMap is {}", persistedMap);
        if (persistedMap != null) {
            String lastRead = (String) persistedMap.get(LAST_READ);
            if (isNotNullOrBlank(lastRead)) {
                fromDate = lastRead;
            }

            Object lastApiOffset = persistedMap.get(LAST_API_OFFSET);
            if (lastApiOffset != null) {
                apiOffset = (Long) lastApiOffset;
            }
        }

    }

    private void setupTaskConfig(Map<String, String> props) {
        baseUrl = props.get(SourceConfig.BASE_URL_CONFIG);
        topic = props.get(SourceConfig.TOPIC_CONFIG);
        interval = Long.parseLong(props.get(POLL_INTERVAL));
    }

    @Override
    public List<SourceRecord> poll() {
        if (System.currentTimeMillis() > (lastExecution + interval)) {
            lastExecution = System.currentTimeMillis();
            return getSourceRecords();
        }
        return Collections.emptyList();
    }

    @Override
    public void stop() {

    }

    private List<SourceRecord> getSourceRecords() {
        List<Event> records = getRecords();
        ArrayList<SourceRecord> sourceRecords = new ArrayList<>();
        for (Event event : records) {
            sourceRecords.add(buildSourceRecord(event, fromDate, apiOffset));
        }
        return sourceRecords;
    }

    List<Event> getRecords() {
        List<Event> responseRecords = SourceClient.getRecords(baseUrl);
        return responseRecords;
    }

    private SourceRecord buildSourceRecord(Event event, String lastRead, Long apiOffset) {
        Map<String, Object> sourceOffset = buildSourceOffset(lastRead, apiOffset);
        Map<String, Object> sourcePartition = buildSourcePartition();
        return new SourceRecord(sourcePartition, sourceOffset, topic, Event.SCHEMA, event.toStruct());
    }

    private Map<String, Object> buildSourceOffset(String lastRead, Long apiOffset) {
        Map<String, Object> sourceOffset = new HashMap<>();
        sourceOffset.put(LAST_READ, lastRead);
        sourceOffset.put(LAST_API_OFFSET, apiOffset);
        return sourceOffset;
    }

    private Map<String, Object> buildSourcePartition() {
        return Collections.singletonMap(URL, baseUrl);
    }
}
