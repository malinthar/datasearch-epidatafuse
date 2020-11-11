package io.datasearch.epidatafuse.core.fusionpipeline.datastore.ingestion;

import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.AttributeUtil;
import io.datasearch.epidatafuse.core.fusionpipeline.datastore.schema.SimpleFeatureTypeSchema;
import io.datasearch.epidatafuse.core.util.IngestConfig;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.feature.FeatureCollection;
import org.geotools.feature.FeatureIterator;
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

import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;


/**
 * Template class for generating features.
 */
public class SimpleFeatureTransformer {

    private static final Logger logger = LoggerFactory.getLogger(SimpleFeatureTransformer.class);
    private static final String DELIMITED_TEXT_TYPE = "delimited-text";
    private static final String SHAPE_FILE_TYPE = "shp";
    private static final String ATTRIBUTE_NAME_KEY = "attribute_name";
    private static final String ATTRIBUTE_TYPE_KEY = "attribute_type";
    private static final String ATTRIBUTE_TRANSFORMATION_KEY = "attribute_transformation";
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final Map<String, CSVFormat> CSV_FORMAT = new HashMap<>();
    private MessageDigest md5;

    static {
        CSV_FORMAT.put("Excel", CSVFormat.EXCEL);
        CSV_FORMAT.put("Default", CSVFormat.DEFAULT);
    }

    public SimpleFeatureTransformer() {
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            logger.error("MD5 could not be instantiated!");
        }
    }

    public int transformAndWrite(DataStore dataStore, IngestConfig ingestConfig,
                                 SimpleFeatureTypeSchema simpleFeatureTypeSchema) throws Exception {
        int counter = 0;
        String sourceType = ingestConfig.getSourceType();
        String sourceFormat = ingestConfig.getSourceFormat();
        FeatureWriter<SimpleFeatureType, SimpleFeature> writer =
                dataStore.getFeatureWriterAppend(ingestConfig.getFeatureName(), Transaction.AUTO_COMMIT);
        Map<String, String> transformations = ingestConfig.getTransformations();
        if (DELIMITED_TEXT_TYPE.equals(sourceType)) {
            for (String dataSource : ingestConfig.getDataSources()) {
                try {
                    URL sourceFileUrl = getClass().getClassLoader().getResource(dataSource);
                    CSVParser parser = CSVParser.parse(sourceFileUrl, DEFAULT_CHARSET, CSV_FORMAT.get(sourceFormat));
                    counter += transformCSV(simpleFeatureTypeSchema, writer, parser, transformations);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        } else if (SHAPE_FILE_TYPE.equals(sourceType)) {
            for (String dataSource : ingestConfig.getDataSources()) {
                try {
                    URL sourceFileUrl = getClass().getClassLoader().getResource(dataSource);
                    Map<String, Object> dataStoreFinderUrlMap = new HashMap<>();
                    dataStoreFinderUrlMap.put("url", sourceFileUrl);
                    DataStore tempDataStore = DataStoreFinder.getDataStore(dataStoreFinderUrlMap);
                    FeatureSource<SimpleFeatureType, SimpleFeature> tempFeatureSource =
                            tempDataStore.getFeatureSource(tempDataStore.getTypeNames()[0]);
                    SimpleFeatureType tempSchema = tempFeatureSource.getSchema();
                    CoordinateReferenceSystem dataCRS = tempSchema.getCoordinateReferenceSystem();
                    CoordinateReferenceSystem worldCRS = DefaultGeographicCRS.WGS84;
                    MathTransform mathTransform = CRS.findMathTransform(dataCRS, worldCRS, true);
                    FeatureCollection<SimpleFeatureType, SimpleFeature> collection = tempFeatureSource.getFeatures();
                    FeatureIterator<SimpleFeature> iterator = collection.features();

                    while (iterator.hasNext()) {
                        SimpleFeature tempNext = iterator.next();
                        SimpleFeature next = writer.next();
                        next.getUserData().put(Hints.PROVIDED_FID, generateFeatureID(tempNext.getID()));
                        for (Map<String, String> attribute : simpleFeatureTypeSchema.getAttributes()) {
                            String attributeName = attribute.get(ATTRIBUTE_NAME_KEY);
                            if (AttributeUtil.getGeometricTypeList().contains(attribute.get(ATTRIBUTE_TYPE_KEY))) {
                                next.setAttribute(attributeName,
                                        JTS.transform((Geometry) tempNext.getDefaultGeometryProperty().getValue(),
                                                mathTransform));
                            } else {
                                int transformationIndex = Integer.parseInt(transformations.get(attributeName));
                                next.setAttribute(attributeName, tempNext.getAttribute(transformationIndex));
                            }
                        }
                        writer.write();
                        counter++;
                        logger.info("Added new record," + next.getID() + " to " +
                                simpleFeatureTypeSchema.getSimpleFeatureTypeName());
                    }
                    iterator.close();
                    tempDataStore.dispose();
                } catch (Throwable e) {
                    logger.error(e.getMessage());
                }
            }

        }
        return counter;
    }

    public int transformCSV(SimpleFeatureTypeSchema schema,
                            FeatureWriter<SimpleFeatureType, SimpleFeature> writer,
                            CSVParser parser, Map<String, String> transformations) throws Exception {
        int counter = 0;
        for (CSVRecord record : parser) {
            SimpleFeature next = writer.next();
            next.getUserData().put(Hints.PROVIDED_FID, generateFeatureID(record.toString()));
            for (Map<String, String> attribute : schema.getAttributes()) {
                String attributeName = attribute.get(ATTRIBUTE_NAME_KEY);
                String attributeType = attribute.get(ATTRIBUTE_TYPE_KEY);
                int columnIndex = Integer.parseInt(transformations.get(attributeName));
                String attributeValue = record.get(columnIndex);
                Object value = AttributeUtil.convert(attributeValue, attributeType);
                next.setAttribute(attribute.get(ATTRIBUTE_NAME_KEY), value);
            }
            writer.write();
            counter++;
            logger.info("Added new Feature," + next.toString() + "to" + schema.getSimpleFeatureTypeName());
        }
        return counter;
    }

    public String generateFeatureID(String record) {
        byte[] recordBytes = record.getBytes(DEFAULT_CHARSET);
        byte[] md5sum = this.md5.digest(recordBytes);
        String featureID = String.format("%032X", new BigInteger(1, md5sum));
        return featureID;
    }
}
//                    } catch (Exception e) {
//                        throw new Exception("Record" + record.get(ATTRIBUTE_NAME_KEY) +
//                    " could not be added: Invalid transformation of attribute.get(ATTRIBUTE_NAME_KEY)", e);
//                    }

//    private static final String SOURCE_TYPE_KEY = "source_type";
//    private static final String FEATURE_NAME_KEY = "feature_name";
//    private static final String SOURCE_FORMAT_KEY = "source_format";
//    private static final String DATA_SOURCE_KEY = "data_sources";

//DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern(simpleFeatureTypeSchema.
//            getAdditionalConfigurations().get("DateTimeFormat"), Locale.US);

//        URL sourceCSVFile;
//        DateTimeFormatter dateFormat = null; //todo: give a default value
//        GeometryFactory geometryFactory = new GeometryFactory();
//
//        Map<String, Integer> records = (Map<String, Integer>) ingestConfig.get("records");
//        Map<String, String> configurations = (Map<String, String>) ingestConfig.get("configurations");
//
//        try {
//            sousourceFile = getClass().getClassLoader().getResource((String) ingestConfig.get("data_source"));
//        } catch (Exception e) {
//            throw new RuntimeException("Couldn't load resource " + ingestConfig.get("data_source"));
//        }
//
//        // Datetime Formatter
//        if (simpleFeatureTypeSchema.getAdditionalConfigurations().get("DateTimeFormat") != null) {
//            dateFormat = DateTimeFormatter.ofPattern(simpleFeatureTypeSchema.
//                    getAdditionalConfigurations().get("DateTimeFormat"), Locale.US);
//        }
//
//        // parser corresponding to the CSV format
//        try (CSVParser parser = CSVParser.parse(sourceCSVFile, StandardCharsets.UTF_8, CSVFormat.EXCEL)) {
//            for (CSVRecord record : parser) {
//                try {
//                    // pull out the fields corresponding to our simple feature attributes
//                    for (Map<String, String> attribute : simpleFeatureTypeSchema.getAttributes()) {
//                        if ("Date".equalsIgnoreCase((attribute.get("attributeType")))) {
//                            builder.set(attribute.get("attributeName"), Date.from(LocalDate.parse(
//                                    record.get(records.get(attribute.get("attributeName"))),
//                                    dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant()));
//                        } else if ("Point".equals(attribute.get("attributeType"))) {
//                            double latitude = Double.parseDouble(
//                                    record.get(records.get("Lat")));
//                            double longitude = Double.parseDouble(
//                                    record.get(records.get("Long")));
//                            builder.set(attribute.get("attributeName"),
//                                    "POINT (" + longitude + " " + latitude + ")");
//                        } else if ("MultiPolygon".equals(attribute.get("attributeType")) ||
//                                "Polygon".equals(attribute.get("attributeType"))) {
//
//                            Map<String, String> shapeFile = (Map<String, String>) ingestConfig.get("shp_file");
//
//                            //A map to create a temporary data store
//                            Map<String, Object> dataStoreFinderUrlMap = new HashMap<>();
//                            dataStoreFinderUrlMap.put("url",
//                                    getClass().getClassLoader().getResource(shapeFile.get("Source")));
//                            DataStore dataStore = DataStoreFinder.getDataStore(dataStoreFinderUrlMap);
//                            FeatureSource<SimpleFeatureType, SimpleFeature> source =
//                                    dataStore.getFeatureSource(simpleFeatureTypeSchema.getTypeName());
//
//                            SimpleFeatureType schema = source.getSchema();
//                            CoordinateReferenceSystem dataCRS = schema.getCoordinateReferenceSystem();
//                            CoordinateReferenceSystem worldCRS = DefaultGeographicCRS.WGS84;
//
//                            /**
//                             *@lenient(true) If the coordinate operations should be created even when there is
//                             * no information available for a datum shift
//                             */
//                            boolean lenient = true;
//                            MathTransform mathTransform = CRS.findMathTransform(dataCRS, worldCRS, lenient);
//                            FeatureCollection<SimpleFeatureType, SimpleFeature> collection = source.getFeatures();
//                            FeatureIterator<SimpleFeature> iterator = collection.features();
//
//                            try {
//                                Geometry geom = geometryFactory.creategetTypeNamePolygon();
//                                while (iterator.hasNext()) {
//                                    SimpleFeature feature = iterator.next();
//                                    if (feature.getAttribute(shapeFile.get("FeatureID")).
//                                            toString().equalsIgnoreCase(
//                                            (record.get(records.get(configurations.get("FeatureID")))))) {
//                                        Geometry tempGeom =
//                                                (Geometry) feature.getDefaultGeometryProperty().getValue();
//                                        geom = JTS.transform(tempGeom, mathTransform);
//                                        break;
//                                    } DataStore dataStore = DataStoreFinder.getDataStore(dataStoreFinderUrlMap);
//                            FeatureSource<SimpleFeatureType, SimpleFeature> source =
//                                    dataStore.getFeatureSource(simpleFeatureTypeSchema.getTypeName());
//                                }
//                                builder.set(attribute.get("attributeName"), geom);
//                            } catch (Throwable e) {
//                                logger.debug("Invalid shp file record: " + e.toString()
//                                        + " " + record.toString());
//                            } finally {
//                                iterator.close();
//                                dataStore.dispose();
//                            }
//                        } else {
//                            builder.set((String) attribute.get("attributeName"),
//                                    record.get(records.get(attribute.get("attributeName"))));
//                        }
//                    }
//                    builder.featureUserData(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);
//                    SimpleFeature feature = builder.buildFeature(
//                            record.get(records.get(simpleFeatureTypeSchema.
//                                    getAdditionalConfigurations().get("FeatureID"))));
//                    features.add(feature);
//                } catch (Throwable e) {
//                    logger.debug("Invalid" + simpleFeatureTypeSchema.getTypeName() +
//                            "record: " + e.toString() + " " + record.toString());
//                }
//            }
//            return features;
//        } catch (IOException e) {
//            throw new RuntimeException("Error reading" + simpleFeatureTypeSchema.getTypeName() + ":", e);
//        }
