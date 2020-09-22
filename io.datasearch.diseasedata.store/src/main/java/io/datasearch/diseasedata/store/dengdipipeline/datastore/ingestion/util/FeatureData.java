package io.datasearch.diseasedata.store.dengdipipeline.datastore.ingestion.util;

import io.datasearch.diseasedata.store.DiseaseDataStore;
import io.datasearch.diseasedata.store.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Geometry;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Template class for generating features.
 */
public class FeatureData {

    private List<SimpleFeature> features = null;
    private String typeName;
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    public FeatureData(Map<String, Object> parameters, Map<String, SimpleFeatureTypeSchema> simpleFeatureTypeSchemas) {
        this.typeName = (String) parameters.get("feature_name");
        //buildSimpleFeature((List<Map<String, String>>) parameters.get("attributes"));
        //buildQueries((List<Map<String, String>>) (parameters.get("queries")));
        buildSimpleFeatureData((String) parameters.get("data_source"),
                (Map<String, Integer>) parameters.get("records"),
                simpleFeatureTypeSchemas.get(this.typeName).getConfigurations(),
                simpleFeatureTypeSchemas.get(this.typeName).getAttributes(),
                simpleFeatureTypeSchemas.get(this.typeName).getSimpleFeatureType(),
                (Map<String, String>) parameters.get("shp_file"));
    }

    public List<SimpleFeature> getFeatureData() {
        return features;
    }

    public void buildSimpleFeatureData(String dataSource, Map<String, Integer> records,
                                       Map<String, String> configurations,
                                       List<Map<String, String>> attributes,
                                       SimpleFeatureType simpleFeatureType,
                                       Map<String, String> shpFile) {
        if (features == null) {

            DateTimeFormatter dateFormat = null;
            List<SimpleFeature> features = new ArrayList<>();
            URL input = getClass().getClassLoader().getResource(dataSource);
            if (input == null) {
                throw new RuntimeException("Couldn't load resource " + dataSource);
            }

            // date parser corresponding to the CSV format
            if (configurations.get("DateTimeFormat") != null) {
                dateFormat = DateTimeFormatter.ofPattern(configurations.get("DateTimeFormat"), Locale.US);
            }

            SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureType);
            try {
                try (CSVParser parser = CSVParser.parse(input, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {

                    for (CSVRecord record : parser) {
                        try {
                            // pull out the fields corresponding to our simple feature attributes
                            for (Map<String, String> attribute : attributes) {
                                if ("Date".equalsIgnoreCase(attribute.get("attributeType"))) {
                                    builder.set(attribute.get("attributeName"),
                                            Date.from(LocalDate.parse(
                                                    record.get(
                                                            records.get(attribute.get("attributeName"))),
                                                    dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
                                } else if ("Point".equals(attribute.get("attributeType"))) {
                                    double latitude = Double.parseDouble(
                                            record.get(records.get("Lat")));
                                    double longitude = Double.parseDouble(
                                            record.get(records.get("Long")));
                                    builder.set(attribute.get("attributeName"),
                                            "POINT (" + longitude + " " + latitude + ")");

                                } else if ("MultiPolygon".equals(attribute.get("attributeType")) ||
                                        "Polygon".equals(attribute.get("attributeType"))) {
                                    URL path = getClass().getClassLoader().getResource(shpFile.get("Source"));
                                    Map<String, Object> map = new HashMap<>();
                                    map.put("url", path);
                                    DataStore dataStore = DataStoreFinder.getDataStore(map);
                                    String typeName = dataStore.getTypeNames()[0];
                                    FeatureSource<SimpleFeatureType, SimpleFeature> source =
                                            dataStore.getFeatureSource(typeName);
                                    SimpleFeatureType schema = source.getSchema();
                                    CoordinateReferenceSystem dataCRS = schema.getCoordinateReferenceSystem();
                                    CoordinateReferenceSystem worldCRS = DefaultGeographicCRS.WGS84;
                                    boolean lenient = true;
                                    MathTransform transform = CRS.findMathTransform(dataCRS, worldCRS, lenient);
                                    FeatureCollection<SimpleFeatureType, SimpleFeature> collection =
                                            source.getFeatures();
                                    FeatureIterator<SimpleFeature> iterator = collection.features();
                                    try {
                                        while (iterator.hasNext()) {
                                            SimpleFeature f = iterator.next();

                                            if (f.getAttribute(shpFile.get("FeatureID")).
                                                    toString().equalsIgnoreCase(
                                                    (record.get(records.get(configurations.get("FeatureID")))))) {
                                                Geometry tempGeom =
                                                        (Geometry) f.getDefaultGeometryProperty().getValue();
                                                Geometry geom = JTS.transform(tempGeom, transform);
                                                if (!geom.isValid()) {
                                                    String name =
                                                            f.getAttribute(shpFile.get("FeatureID")).toString();
                                                    logger.error(
                                                            "Invalid geometry shape found on " + name + " moh area.");
                                                    break;
                                                }
                                                builder.set(attribute.get("attributeName"), geom);
                                                break;
                                            } else {
                                                continue;
                                            }
                                        }

                                    } catch (Throwable e) {
                                        logger.debug("Invalid shp file record: " + e.toString()
                                                + " " + record.toString());
                                    } finally {
                                        iterator.close();
                                        dataStore.dispose();
                                    }

                                } else {
                                    builder.set(attribute.get("attributeName"),
                                            record.get(records.get(attribute.get("attributeName"))));
                                }
                            }
                            builder.featureUserData(Hints.USE_PROVIDED_FID, Boolean.TRUE);
                            SimpleFeature feature = builder.buildFeature(
                                    record.get(records.get(configurations.get("FeatureID"))));
                            features.add(feature);
                        } catch (Throwable e) {
                            throw new Exception(e);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Error reading weather-data:", e);
                }
            } catch (Exception e) {
                logger.error("Error found in shape file source " + e.toString());
            }
            this.features = Collections.unmodifiableList(features);
        }
    }
}
