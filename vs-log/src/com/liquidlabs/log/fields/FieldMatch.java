package com.liquidlabs.log.fields;


class FieldMatch {
    private String guessedName;
    private String regex;

    public FieldMatch(String guessedName, String regex) {
        this.guessedName = guessedName;
        this.regex = regex;
    }


    public FieldMatch mergeWith(FieldMatch other) {
        if (equals(other)) {
            return this;
        }
        return AutoFieldGuesser.TheRest;
    }

    public String guessedName() {
        return guessedName;
    }

    public String regex() {
        return regex;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof FieldMatch && ((FieldMatch) o).regex.equals(regex);
    }

    @Override
    public int hashCode() {
        return 31 * getClass().hashCode() + regex.hashCode();
    }
}
