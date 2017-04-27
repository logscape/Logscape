package com.liquidlabs.log.fields;

import org.apache.log4j.Logger;
import org.joda.time.DateTimeUtils;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class FieldSetAssember {
	static final Logger LOGGER = Logger.getLogger(FieldSetAssember.class);

	public FieldSet determineFieldSet(String fullFilePath, List<FieldSet> fieldSets, List<String> lines, boolean sortFieldSets, String tags) throws Exception {
		
		try {
			
//			LOGGER.info(">> CHECKING:" + fullFilePath + " FSets:" + fieldSets.size());
			if (sortFieldSets) sortFieldSets(fieldSets);
		
			
			for (FieldSet fieldSet : fieldSets) {
				try {
					if (FieldSetUtil.validate(fieldSet, fullFilePath, lines, tags)) {
						return fieldSet;
					}
				} catch (Throwable t) {
					LOGGER.warn("FieldSet validate Failed:" + fieldSet.id + " ex:" + t);
				}
                fieldSet.getKVStore().commit();
			}

            return FieldSets.getBasicFieldSet();
		} catch (Throwable t) {
			LOGGER.error("Failed to find fieldSet for:" + fullFilePath, t);
            return FieldSets.getBasicFieldSet();
		} finally {
			long end = DateTimeUtils.currentTimeMillis();
			//LOGGER.info("<< CHECKING:" + fullFilePath + " e:" + (end - start));			
		}
	}

	private void sortFieldSets(List<FieldSet> fieldSets) {
		// sort according to precendence
		Collections.sort(fieldSets, new Comparator<FieldSet>(){

			public int compare(FieldSet o1, FieldSet o2) {
				return Integer.valueOf(o2.priority).compareTo(o1.priority);
			}
		});
	}

}
