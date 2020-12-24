package io.datasearch.epidatafuse.core.fusionpipeline.fuseengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.ArrayList;

/**
 * For aggregating data.
 */
public class DataFrameBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DataFrameBuilder.class);

//    public DataFrame createDataFrame(String dtg) {
//        return new DataFrame(dtg);
//    }
//
//    public void buildDataFrame(AggregatedCollection aggregatedCollection) {
//
//        ArrayList<String> finalAttributeList = aggregatedCollection.getAttributeList();
//        SimpleFeatureCollection collection = aggregatedCollection.getFeatureCollection();
//
//        ArrayList<String> csvRecords = new ArrayList<String>();
//
//        String fileName =
//                aggregatedCollection.getFeatureTypeName() + "_" + aggregatedCollection.getSpatialGranularity() + "_" +
//                        aggregatedCollection.getTemporalGranularity() + "_" + aggregatedCollection.getDtg();
//
//        SimpleFeatureIterator iterator = collection.features();
//
//        while (iterator.hasNext()) {
//            SimpleFeature feature = iterator.next();
//
//            ArrayList<String> csvLine = new ArrayList<String>();
//            csvLine.add(feature.getID());
//
//            for (String attribute : finalAttributeList) {
//                if (feature.getAttribute(attribute) != null) {
//                    csvLine.add(feature.getAttribute(attribute).toString());
//                }
//            }
////            logger.info(csvLine.toString());
//            String joined = String.join(",", csvLine);
//            logger.info(joined);
//            csvRecords.add(joined);
////            logger.info(System.getProperty("user.dir"));
//        }
//        this.writeToCSV(csvRecords, fileName);
//    }

    public void writeToCSV(ArrayList<String> csvRecords, String fileName) {
        String dir = "public/output/";
        String csvFileName = dir + fileName + ".csv";
        try {
            File file = new File(csvFileName);
            Writer w = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            PrintWriter pw = new PrintWriter(w);
            for (String record : csvRecords) {
                pw.println(record);
            }
            pw.close();
        } catch (Throwable e) {
            logger.error(e.getMessage());
        }
    }
}
