package com.liquidlabs.log.fields;

enum FieldDelimeter {
    Space("\\s+", "([^\\s]+)"), Comma(",", "([^,]+)");

    private final String delim;
    private final String defaultMatch;

    FieldDelimeter(String delim, String defaultMatch) {
        this.delim = delim;
        this.defaultMatch = defaultMatch;
    }

    public String defaultMatch() {
        return defaultMatch;
    }

    public String delim() {
        return delim;
    }
}
