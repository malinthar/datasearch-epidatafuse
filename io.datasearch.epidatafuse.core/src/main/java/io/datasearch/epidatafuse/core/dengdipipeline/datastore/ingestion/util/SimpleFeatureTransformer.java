package io.datasearch.epidatafuse.core.dengdipipeline.datastore.ingestion.util;

import io.datasearch.epidatafuse.core.DiseaseDataStore;
import io.datasearch.epidatafuse.core.dengdipipeline.datastore.schema.SimpleFeatureTypeSchema;
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

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Template class for generating features.
 */
public class SimpleFeatureTransformer {
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);

    public List<SimpleFeature> transform(Map<String, Object> sourceConfig,
                                         SimpleFeatureTypeSchema simpleFeatureTypeSchema) {

        Map<String, Integer> records = (Map<String, Integer>) sourceConfig.get("records");
        Map<String, String> shpFile = (Map<String, String>) sourceConfig.get("shp_file");
        Map<String, String> configurations = (Map<String, String>) sourceConfig.get("configurations");
        DateTimeFormatter dateFormat = null;
        List<SimpleFeature> features = new ArrayList<>();
        URL sourceCSVFile;
        SimpleFeatureBuilder builder = new SimpleFeatureBuilder(simpleFeatureTypeSchema.getSimpleFeatureType());

        // Load from csv file
        try {
            sourceCSVFile = getClass().getClassLoader().getResource((String) sourceConfig.get("data_source"));
        } catch (Exception e) {
            throw new RuntimeException("Couldn't load resource " + sourceConfig.get("data_source"));
        }

        // Datetime Formatter
        if (simpleFeatureTypeSchema.getConfigurations().get("DateTimeFormat") != null) {
            dateFormat = DateTimeFormatter.ofPattern(simpleFeatureTypeSchema.
                    getConfigurations().get("DateTimeFormat"), Locale.US);
        }

        // parser corresponding to the CSV format
        try (CSVParser parser = CSVParser.parse(sourceCSVFile, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
            for (CSVRecord record : parser) {
                try {
                    // pull out the fields corresponding to our simple feature attributes
                    for (Map<String, Object> attribute : simpleFeatureTypeSchema.getAttributes()) {
                        if ("Date".equalsIgnoreCase((String) attribute.get("attributeType"))) {
                            builder.set((String) attribute.get("attributeName"), Date.from(LocalDate.parse(
                                    record.get(records.get(attribute.get("attributeName"))),
                                    dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
                        } else if ("Point".equals(attribute.get("attributeType"))) {
                            double latitude = Double.parseDouble(
                                    record.get(records.get("Lat")));
                            double longitude = Double.parseDouble(
                                    record.get(records.get("Long")));
                            builder.set((String) attribute.get("attributeName"),
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
                                        builder.set((String) attribute.get("attributeName"), geom);
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
                            builder.set((String) attribute.get("attributeName"),
                                    record.get(records.get(attribute.get("attributeName"))));
                        }
                    }
                    builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
                    SimpleFeature feature = builder.buildFeature(
                            record.get(records.get(simpleFeatureTypeSchema.
                                    getConfigurations().get("FeatureID"))));
                    features.add(feature);
                } catch (Throwable e) {
                    logger.debug("Invalid" + simpleFeatureTypeSchema.getTypeName() +
                            "record: " + e.toString() + " " + record.toString());
                }
            }
            return features;
        } catch (IOException e) {
            throw new RuntimeException("Error reading" + simpleFeatureTypeSchema.getTypeName() + ":", e);
        }

    }
}
