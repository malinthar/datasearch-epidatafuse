package io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion.util;

import io.datasearch.epidatafuse.core.dengdipipeline.datastore.PipelineDataStore;
import io.datasearch.epidatafuse.core.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.siddhi.core.event.Event;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.util.factory.Hints;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transform Streaming Siddhi events to SimpleFeatures.
 */
public class EventTransformer {
    private static final Logger logger = LoggerFactory.getLogger(PipelineDataStore.class);

    public List<SimpleFeature> transform(Event[] events,
                                         SimpleFeatureTypeSchema simpleFeatureTypeSchema) {
        List<SimpleFeature> features = new ArrayList<>();
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
                simpleFeatureTypeSchema.getSimpleFeatureType());
        Map<String, String> record = new HashMap<>();

        //String[] attributes = {"StationName", "Longitude", "Latitude", "dtg", "ObservedValue"};

        //filter out attributes which are not derived
        //todo: add this to schema
        List<Map<String, Object>> attributes = simpleFeatureTypeSchema.getAttributes()
                .stream()
                .filter(attr -> ((Integer) attr.get("derived")) == 0)
                .collect(Collectors.toList());

        // Datetime Formatter
        DateTimeFormatter dateFormat = null;
        if (simpleFeatureTypeSchema.getConfigurations().get("DateTimeFormat") != null) {
            dateFormat = DateTimeFormatter.ofPattern(simpleFeatureTypeSchema.
                    getConfigurations().get("DateTimeFormat"), Locale.US);
        }

        //todo: assuming the order is preserved
        for (Event event : events) {
            int index = 0;
            if (attributes.size() == event.getData().length) {
                for (Map<String, Object> attribute : attributes) {
                    record.put((String) attribute.get("attributeName"), (String) event.getData(index));
                    index++;
                }
            } else {
                logger.error("Event dropped: Schema and input mismatch '" + simpleFeatureTypeSchema + "'");
            }
        }

        for (Map<String, Object> attribute : simpleFeatureTypeSchema.getAttributes()) {
            if ("Date".equalsIgnoreCase((String) attribute.get("attributeType"))) {
                builder.set((String) attribute.get("attributeName"),
                        Date.from(LocalDate.parse(record.get(attribute.get("attributeName")),
                                dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
            } else if ("Point".equals(attribute.get("attributeType"))) {
                double latitude = Double.parseDouble(
                        record.get("Latitude"));
                double longitude = Double.parseDouble(
                        record.get("Longitude"));
                builder.set((String) attribute.get("attributeName"),
                        "POINT (" + longitude + " " + latitude + ")");
            } else {
                builder.set((String) attribute.get("attributeName"),
                        record.get(attribute.get("attributeName")));
            }
        }
        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
        SimpleFeature feature = builder.buildFeature(
                record.get(simpleFeatureTypeSchema.getConfigurations().get("FeatureID")));

        features.add(feature);
        return features;
    }
}
