package io.datasearch.denguestore.data;

import io.datasearch.denguestore.DiseaseDataStore;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.type.AttributeTypeImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.factory.Hints;
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Template class for generating features.
 */
public class FeatureData implements DiseaseData {
    private SimpleFeatureType sft = null;
    private List<SimpleFeature> features = null;
    private String typeName;
    private List<Query> queries = null;
    private Filter subsetFilter = null;
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    public FeatureData(Map<String, Object> parameters) {
        this.typeName = (String) parameters.get("feature_name");
        buildSimpleFeature((List<String>) parameters.get("attributes"));
        buildQueries((List<Object>) (parameters.get("queries")));
        buildTestData((String) parameters.get("data_source"));
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }

    @Override
    public SimpleFeatureType getSimpleFeatureType() {
        return sft;
    }

    @Override
    public List<SimpleFeature> getTestData() {
        return features;
    }

    @Override
    public List<Query> getTestQueries() {
        return queries;
    }

    @Override
    public Filter getSubsetFilter() {
        if (subsetFilter == null) {
            // Get a FilterFactory2 to build up our query
            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

            // most of the data is from 2018-01-01
            ZonedDateTime dateTime = ZonedDateTime.of(2018, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC);
            Date start = Date.from(dateTime.minusDays(1).toInstant());
            Date end = Date.from(dateTime.plusDays(1).toInstant());

            // note: BETWEEN is inclusive, while DURING is exclusive
            Filter dateFilter = ff.between(ff.property("dtg"), ff.literal(start), ff.literal(end));

            // bounding box over small portion of the eastern United States
            Filter spatialFilter = ff.bbox("geom", -83, 33, -80, 35, "EPSG:4326");

            // Now we can combine our filters using a boolean AND operator
            subsetFilter = ff.and(dateFilter, spatialFilter);

            // note the equivalent using ECQL would be:
            // ECQL.toFilter("bbox(geom,-83,33,-80,35) AND dtg between
            // '2017-12-31T00:00:00.000Z' and '2018-01-02T00:00:00.000Z'");
        }
        return subsetFilter;
    }

    public void buildSimpleFeature(List<String> attributes) {
        if (sft == null) {
            StringBuilder featureAttributes = new StringBuilder();
            for (int i = 0; i < attributes.size(); i++) {
                featureAttributes.append(attributes.get(i));
                if (i != attributes.size() - 1) {
                    featureAttributes.append(",");
                }
            }
            sft = SimpleFeatureTypes.createType(getTypeName(), featureAttributes.toString());
        }
    }

    public void buildQueries(List<Object> queriesList) {
        if (queries == null) {
            try {
                List<Query> queries = new ArrayList<>();
                for (Object queryData : queriesList) {
                    queries.add(new Query(getTypeName(),
                            ECQL.toFilter(((Map<String, String>) queryData).get("query"))));
                }
                this.queries = Collections.unmodifiableList(queries);
            } catch (CQLException e) {
                throw new RuntimeException("Error creating filter:", e);
            }
        }
    }

    public void buildTestData(String dataSource) {
        if (features == null) {

            List<SimpleFeature> features = new ArrayList<>();
            URL input = getClass().getClassLoader().getResource(dataSource);
            if (input == null) {
                throw new RuntimeException("Couldn't load resource weather-rainfall-data.CSV");
            }

            // date parser corresponding to the CSV format
            DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.US);

            // parser corresponding to the CSV format
            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(getSimpleFeatureType());

            try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
                for (CSVRecord record : parser) {
                    try {
                        List<AttributeDescriptor> descriptors = getSimpleFeatureType().getAttributeDescriptors();
                        // pull out the fields corresponding to our simple feature attributes
                        for (int i = 0; i < descriptors.size(); i++) {
                            AttributeTypeImpl  type = (AttributeTypeImpl) descriptors.get(i).getType();
                            logger.info(type.getUserData().toString());
                            if ("dtg".equals(descriptors.get(i).getName())) {
                                builder.set("dtg",
                                        Date.from(LocalDate.parse(record.get(i),
                                                dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
                            } else if ("geom".equals(descriptors.get(i).getName())) {
                                double latitude = Double.parseDouble(record.get(2));
                                double longitude = Double.parseDouble(record.get(3));
                                builder.set("geom", "POINT (" + longitude + " " + latitude + ")");
                            } else {
                                builder.set(descriptors.get(i).getName(), record.get(i));
                            }
                        }
                        builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
                        SimpleFeature feature = builder.buildFeature(record.get(0));
                        features.add(feature);
                    } catch (Throwable e) {
                        logger.debug("Invalid weather-data record: " + e.toString() + " " + record.toString());
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading weather-data:", e);
            }
            this.features = Collections.unmodifiableList(features);
        }
    }
}
