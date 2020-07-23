package io.datasearch.diseasedata.store;

import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLine;
import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLineFactory;
import io.datasearch.diseasedata.store.dengdipipeline.fuseengine.FuseEngine;
import io.datasearch.diseasedata.store.dengdipipeline.fuseengine.GranularityConvertor;
import io.datasearch.diseasedata.store.query.QueryManager;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * DiseaseDataStore is the main class of the DataStore.
 */
public class DiseaseDataStore {
    private static final Logger logger = LoggerFactory.getLogger(DiseaseDataStore.class);
    public static void main(String[] args) {
        BasicConfigurator.configure();
        try {
            if ("create".equalsIgnoreCase(args[args.length - 1])) {
                String[] params = {args[0], args[1], args[2], args[3]};
                DengDIPipeLine pipeLine = DengDIPipeLineFactory.createDengDIPipeLine(params);
                //Insert
                QueryManager queryManager = new QueryManager();
                queryManager.runQueries(pipeLine);

                FuseEngine fuseEngine = pipeLine.getFuseEngine();
                GranularityConvertor granularityConvertor = fuseEngine.getGranularityConvertor();

                List<Map<String, Object>> featureGranularities =  granularityConvertor.loadFeatureGranularities();
                Map<String, Object> aggrigationGranularities =  granularityConvertor.loadAggrigationGranularities();

                logger.info(featureGranularities.toString());
                logger.info(aggrigationGranularities.toString());

//            } else if ("query".equalsIgnoreCase(args[args.length - 1])) {
//                QueryManager queryManager = new QueryManager();
//                queryManager.runQueries();
            } else if ("insert".equalsIgnoreCase(args[args.length - 1])) {
                logger.error("Insert");
            } else if ("convert".equalsIgnoreCase(args[args.length - 1])) {
                logger.error("convert");
            } else {
                logger.error("No option provided!\noptions : query, create, insert");
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            System.exit(1);
        }
        System.exit(0);
    }
}
