package io.datasearch.epidatafuse.core.dengdipipeline.fuseengine;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * For scheduling processes.
 */
public class Scheduler extends TimerTask {

    private static final Logger logger = LoggerFactory.getLogger(Scheduler.class);
    private FuseEngine fuseEngine;

    public void setFuseEngine(FuseEngine fuseEngine) {
        this.fuseEngine = fuseEngine;
    }

    public void run() {
//        Date now = now = new Date();
//        logger.info(now.toString());
        this.fuseEngine.invokeAggregationProcess();
    }
}
