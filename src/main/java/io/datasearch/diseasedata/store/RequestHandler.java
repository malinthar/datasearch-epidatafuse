package io.datasearch.diseasedata.store;

import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLine;
import io.datasearch.diseasedata.store.dengdipipeline.DengDIPipeLineFactory;
import io.datasearch.diseasedata.store.query.QueryManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * Http request controller.
 */
@RestController
public class RequestHandler {
    private static final Logger logger = LoggerFactory.getLogger(RequestHandler.class);
    private Map<String, DengDIPipeLine> dengDIPipeLineMap = new HashMap<>();

    @RequestMapping("/init")
    public String initDengDIPipeline() {
        try {
            String pipelineName = "dengue";
            DengDIPipeLine pipeLine = DengDIPipeLineFactory.createDengDIPipeLine();
            dengDIPipeLineMap.put(pipelineName, pipeLine);
            return "Success!";
        } catch (Exception e) {
            logger.error(e.getMessage());
            return e.getMessage();
        }
    }

    @RequestMapping("/query")
    public String queryDengDIpipeline(String[] params) {
        try {
            String pipelineName = "dengue";
            QueryManager queryManager = new QueryManager();
            queryManager.runQueries(dengDIPipeLineMap.get(pipelineName));
            return "Success!";
        } catch (Exception e) {
            logger.error(e.getMessage());
            return e.getMessage();
        }
    }

    @RequestMapping("/ingest")
    public String ingestDengDIpipeline() {
        try {
            String pipelineName = "dengue";
            dengDIPipeLineMap.get(pipelineName).ingest();
            return "Success";
        } catch (Exception e) {
            logger.error(e.getMessage());
            return e.getMessage();
        }
    }

    @RequestMapping("/granularityMap")
    public String granularityMap() {
        try {
            String pipelineName = "dengue";
            dengDIPipeLineMap.get(pipelineName).mapGranularityRelations();
            return "Success mapping";
        } catch (Exception e) {
            logger.error(e.getMessage());
            return e.getMessage();
        }
    }

}
