package com.nativelogix.rdbms2marklogic.service.migration;

/**
 * Carries a single generated document string and its target URI for writing to MarkLogic.
 */
public class DocumentBuildResult {

    private final String uri;
    private final String content;
    private final String format; // "XML" or "JSON"

    public DocumentBuildResult(String uri, String content, String format) {
        this.uri = uri;
        this.content = content;
        this.format = format;
    }

    public String getUri() { return uri; }
    public String getContent() { return content; }
    public String getFormat() { return format; }
}
