package com.liquidlabs.log.search;

import com.liquidlabs.log.fields.FieldSet;
import com.liquidlabs.log.fields.FieldSets;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.log.search.functions.Function;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class QueryTest {

    @Test
    public void extractPosAggFunctions() throws Exception {
        FieldSet fieldSet = FieldSets.getBasicFieldSet();
        fieldSet.addDefaultFields("basic","host","filename","path","tag","agent","",1,true);
        Query query = new Query(0, 0, ".*?WARN.*","WARN", false);
        query.addFunction(new Count("TAG", "host","_filename+_host"));
        query.addFunction(new Count("MAX", "host","TAG"));
        List<Function> functions = query.functions();
        assertEquals(2, functions.size());
        List<Function> functions2 = query.functions();
        assertEquals(2, functions2.size());

        assertTrue(functions.get(1) == functions2.get(1) );
    }

        @Test
    public void shouldFilterWithConcatedFields() throws Exception {
        FieldSet fieldSet = FieldSets.getBasicFieldSet();
        fieldSet.addDefaultFields("basic", "host", "filename", "path", "tag", "agent", "", 1, true);
        Query query = new Query(0, 0, ".*?WARN.*","WARN", false);
        query.addFunction(new Count("TAG", "host", "_filename+_host"));

        assertTrue(query.containsFields(fieldSet));
    }

	
	@Test
	public void shouldGetHoldOfMatchResult() throws Exception {
		FieldSet fieldSet = FieldSets.getBasicFieldSet();
		Query query = new Query(0,  ".*?WARN.*");
		assertTrue(query.isMatching("line WARN 1").isMatch());
	}

}
