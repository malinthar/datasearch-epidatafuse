package io.datasearch.epidatafuse.core.util;

import java.util.ArrayList;

/**
 * Response model for http requests.
 */

public class OutputFrame {
    String fileName;
    String[] headers;
    ArrayList<String[]> content;

    public OutputFrame(String fileName, String[] headers, ArrayList<String[]> content) {
        this.fileName = fileName;
        this.headers = headers;
        this.content = content;
    }

    public String[] getHeaders() {
        return headers;
    }

    public ArrayList<String[]> getContent() {
        return content;
    }

    public String getFileName() {
        return fileName;
    }
}
