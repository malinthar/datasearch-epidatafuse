package io.datasearch.epidatafuse.core;

import java.util.Map;

/**
 * Response model for http requests.
 */
public class Response {
    private Boolean success;
    private Boolean error;
    private String message;
    private Map<String, Object> data;

    public Response(Boolean success, Boolean error,
                           String message, Map<String, Object> data) {
        this.success = success;
        this.error = error;
        this.data = data;
        this.message = message;
    }

    public Boolean getSuccess() {
        return success;
    }

    public Boolean getError() {
        return error;
    }

    public String getMessage() {
        return message;
    }

    public Map<String, Object> getData() {
        return data;
    }
}
