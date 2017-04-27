package com.liquidlabs.log.fields;


public class DelegatingFieldGenerator implements FieldGenerator{

    private final String id;
    private final int priority;
    private final String dirPath;
    private final String fileMask;

    public DelegatingFieldGenerator(String id, int priority, String dirPath, String fileMask) {
        this.id = id;
        this.priority = priority;
        this.dirPath = dirPath;
        this.fileMask = fileMask;
    }

    public FieldSet guess2(String[] text) {
        FieldSet fieldSet = synthGenerator(text).guess(delegateTo(fileMask, text).guess(text), text);
        fieldSet.id = id;
        fieldSet.filePathMask = dirPath;
        fieldSet.fileNameMask = fileMask;
        fieldSet.priority = priority;
        return fieldSet;

    }
    public FieldSet guess(String...text) {
        FieldSet fieldSet = synthGenerator(text).guess(delegateTo(fileMask, text).guess(text), text);
        fieldSet.id = id;
		fieldSet.filePathMask = dirPath;
		fieldSet.fileNameMask = fileMask;
		fieldSet.priority = priority;
        return fieldSet;
    }

    private SynthFieldGenerator synthGenerator(String...data) {
       boolean useNewGuesser = data[0].startsWith("#new");
		if (useNewGuesser) {
			return new SynthFieldGuesser(0.3);
        }
        return new SynthGuesser();
    }

    private FieldGenerator delegateTo(String fileMask, String... data) {
        if (fileMask. endsWith(".csv")) {
            return new CsvFieldGenerator();
        }
        boolean useNewGuesser = data[0].startsWith("#new");
		if (useNewGuesser) {
			return new AutoFieldGuesser(withDelimiterFrom(lineToCheck(data)));
        }
        return new FieldSetGuesser2();
    }


    private FieldDelimeter withDelimiterFrom(String line) {
       int comma = line.split(FieldDelimeter.Comma.delim()).length;
       int space = line.split(FieldDelimeter.Space.delim()).length;
       return comma > space ? FieldDelimeter.Comma : FieldDelimeter.Space;
    }

    private String lineToCheck(String [] data) {
        return data.length > 1 ? data[1] : data[0];
    }

}
