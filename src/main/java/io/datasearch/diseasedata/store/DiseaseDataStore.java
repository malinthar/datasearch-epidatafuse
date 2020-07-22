package io.datasearch.diseasedata.store;

import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLine;
import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLineFactory;
import io.datasearch.diseasedata.store.query.QueryManager;
import org.apache.log4j.BasicConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
//            } else if ("query".equalsIgnoreCase(args[args.length - 1])) {
//                QueryManager queryManager = new QueryManager();
//                queryManager.runQueries();
            } else if ("insert".equalsIgnoreCase(args[args.length - 1])) {
                logger.error("Insert");
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
