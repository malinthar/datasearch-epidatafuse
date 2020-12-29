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
import java.nio.file.Paths;
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
        Map<String, Integer> transformations = ingestConfig.getTransformations();
        if (DELIMITED_TEXT_TYPE.equals(sourceType)) {
            for (String dataSource : ingestConfig.getDataSources()) {
                try {
                    URL sourceFileUrl = getClass().getClassLoader().getResource(dataSource);
                    CSVParser parser = CSVParser.parse(sourceFileUrl, DEFAULT_CHARSET,
                            CSV_FORMAT.get(sourceFormat).withHeader().withSkipHeaderRecord());
                    counter += transformCSV(simpleFeatureTypeSchema, writer, parser, transformations);
                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }
        } else if (SHAPE_FILE_TYPE.equals(sourceType)) {
            for (String dataSource : ingestConfig.getDataSources()) {
                try {
                    URL sourceFileUrl = Paths.get("public", "uploads",
                            ingestConfig.getPipelineName(),
                            simpleFeatureTypeSchema.getSimpleFeatureTypeName(),
                            "shapefile", dataSource).toUri().toURL();
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
//                        next.setDefaultGeometry(
//                                JTS.transform((Geometry) tempNext.getDefaultGeometryProperty().getValue(),
//                                        mathTransform));
                        for (Map<String, String> attribute : simpleFeatureTypeSchema.getAttributes()) {
                            String attributeName = attribute.get(ATTRIBUTE_NAME_KEY);
                            if (AttributeUtil.getGeometricTypeList().contains(attribute.get(ATTRIBUTE_TYPE_KEY))) {
                                next.setAttribute(attributeName,
                                        JTS.transform((Geometry) tempNext.getDefaultGeometryProperty().getValue(),
                                                mathTransform));
                            } else {
                                int transformationIndex = transformations.get(attributeName);
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
        writer.close();
        return counter;
    }

    public int transformCSV(SimpleFeatureTypeSchema schema,
                            FeatureWriter<SimpleFeatureType, SimpleFeature> writer,
                            CSVParser parser, Map<String, Integer> transformations) throws Exception {
        int counter = 0;
        for (CSVRecord record : parser) {
            SimpleFeature next = writer.next();
            next.getUserData().put(Hints.PROVIDED_FID, generateFeatureID(record.toString()));
            for (Map<String, String> attribute : schema.getAttributes()) {
                String attributeName = attribute.get(ATTRIBUTE_NAME_KEY);
                String attributeType = attribute.get(ATTRIBUTE_TYPE_KEY);
                int transformationIndex = transformations.get(attributeName);
                String attributeValue = record.get(transformationIndex);
                Object value = AttributeUtil.convert(attributeValue, attributeType);
                next.setAttribute(attributeName, value);
            }
            writer.write();
            counter++;
            logger.info("Added new Feature," + next.toString() + " to " + schema.getSimpleFeatureTypeName());
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
