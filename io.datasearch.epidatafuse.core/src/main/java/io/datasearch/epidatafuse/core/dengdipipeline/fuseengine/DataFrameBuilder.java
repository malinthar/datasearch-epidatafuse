package io.datasearch.epidatafuse.core.dengdipipeline.fuseengine;

import io.datasearch.epidatafuse.core.dengdipipeline.DengDIPipeLine;
import io.datasearch.epidatafuse.core.dengdipipeline.models.datamodels.AggregatedCollection;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For aggregating data.
 */
public class DataFrameBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DataFrameBuilder.class);

    public void buildDataFrame(AggregatedCollection aggregatedCollection) {

        ArrayList<String> finalAttributeList = aggregatedCollection.getAttributeList();
        SimpleFeatureCollection collection = aggregatedCollection.getFeatureCollection();

        ArrayList<String> csvRecords = new ArrayList<String>();

        String fileName =
                aggregatedCollection.getFeatureTypeName() + "_" + aggregatedCollection.getSpatialGranularity() + "_" +
                        aggregatedCollection.getTemporalGranularity() + "_" + aggregatedCollection.getDtg();

        SimpleFeatureIterator iterator = collection.features();

        while (iterator.hasNext()) {
            SimpleFeature feature = iterator.next();

            ArrayList<String> csvLine = new ArrayList<String>();
            csvLine.add(feature.getID());

            for (String attribute : finalAttributeList) {
                if (feature.getAttribute(attribute) != null) {
                    csvLine.add(feature.getAttribute(attribute).toString());
                }
            }
//            logger.info(csvLine.toString());
            String joined = String.join(",", csvLine);
            logger.info(joined);
            csvRecords.add(joined);
//            logger.info(System.getProperty("user.dir"));
        }
        this.writeToCSV(csvRecords, fileName);
    }

    public void writeToCSV(ArrayList<String> csvRecords, String fileName) {
        String csvFileName = fileName + ".csv";
        try {
            FileWriter writer = new FileWriter(csvFileName);
            for (String record : csvRecords) {
                writer.write(record + "\n");
            }
            writer.close();
        } catch (Exception e) {
            e.getMessage();
        }


    }
}
