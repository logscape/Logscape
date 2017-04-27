package com.liquidlabs.log.fields;

import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.fields.field.LiteralField;

import java.io.IOException;
import java.util.List;

/**
 * Created by neil on 14/07/2015.
 */
public class EnhancerCSV {
    public static void enhance(FieldSet fieldSet) {
        // open the file to get the first couple of lines out of it to build fields.
        // set the pattern extractor to "," so it splits the lines.
        String type = ((LiteralField)fieldSet.getField(FieldSet.DEF_FIELDS._type.name())).getValue();
        String filename = ((LiteralField)fieldSet.getField(FieldSet.DEF_FIELDS._path.name())).getValue();
        try {
            List<String> strings = FileUtil.readLines(filename, 10);
            createIt(fieldSet, filename, strings);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static FieldSet createIt(FieldSet fieldSet, String filename, List<String> strings) {
        String fieldsString =  strings.get(0);
        String [] fields = null;
//        if (fieldsString.startsWith("#")) {
            fields = fieldsString.split(",");
//        }
        fieldSet.expression = "split(,)";
        fieldSet.fields.remove(0);
        int group = 1;
        for (String field : fields) {
            fieldSet.addField(field.replace("#", "").replace(" ","-").replace("\"","").trim(), "count()", true, group++, true, "","", "", false);
        }
        fieldSet.buildFieldNameCache();
        return fieldSet;
    }
}
