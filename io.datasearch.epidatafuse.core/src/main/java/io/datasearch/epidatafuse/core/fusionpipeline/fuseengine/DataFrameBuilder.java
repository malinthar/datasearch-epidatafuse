package io.datasearch.epidatafuse.core.fusionpipeline.fuseengine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

/**
 * For aggregating data.
 */
public class DataFrameBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DataFrameBuilder.class);

    public void writeToCSV(ArrayList<String> csvRecords, String fileName, String pipelineName) {
        Path rootDir = Paths.get("public", "output", pipelineName);

        String dir = "public/output/" + pipelineName;
        String csvFileName = dir + fileName + ".csv";
        try {
            if (!Files.exists(rootDir)) {
                Files.createDirectories(rootDir);
            }
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
