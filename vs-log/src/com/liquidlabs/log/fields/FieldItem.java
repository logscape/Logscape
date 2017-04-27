package com.liquidlabs.log.fields;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.liquidlabs.common.file.FileUtil;
import com.liquidlabs.log.LogProperties;
import com.liquidlabs.log.fields.field.FieldI;
import com.liquidlabs.log.search.functions.*;
import com.liquidlabs.log.search.handlers.SummaryBucket;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.log4j.Logger;

import java.util.*;

import static java.util.Collections.*;

public class FieldItem {
	private final static Logger LOGGER = Logger.getLogger(FieldItem.class);
	public static final int MAX_FIELDS_TO_DISPLAY = Integer.getInteger("max.fields.display", 20);

	public String fieldSetId;
	public String fieldName;
	public String info = "";
	public String fieldLabelWithCount = "";
	public String total = "0";
	public String funct = "count()";
	public boolean visible = true;
	public boolean summary = true;
	int pos = 100;
	public List<KeyValue> keyValues = new ArrayList<KeyValue>();
    public boolean dynamic;

    public FieldItem(String name) {
		this.fieldName = name;
		this.fieldLabelWithCount = name;
	}

	public FieldItem(FieldI field, String fieldSetId) {
		this.fieldName = field.name();
		this.fieldLabelWithCount = field.name();
		this.visible = field.isVisible();
		this.summary = field.isSummary();
		this.fieldSetId = fieldSetId;
		this.info = field.description();
		this.funct = field.funct();
	}


	public static List<FieldItem> convert(Map<String, Map> funcResults, FieldSet fieldSet, String fieldSetId, boolean isKeyValueExtracting) {
        ArrayList<FieldItem> resultList = new ArrayList<FieldItem>();

        try {
            if (funcResults == null) {

                List<FieldI> fields = fieldSet.fields();
                for (FieldI field : fields) {
                    FieldItem newFieldItem = new FieldItem(field, fieldSetId);

                    resultList.add(newFieldItem);
                }
                return resultList;
            }

            List<FieldI> fields = fieldSet.fields();
            List<FieldItem> remainder = new ArrayList<FieldItem>();
            int typeCount = 0;
            for (FieldI field : fields) {
                try {
                    String fieldName = field.name();
                    FieldItem fieldItem = new FieldItem(field, fieldSetId);
                    Map countUniqueIntMap = funcResults.get(new CountFaster(fieldName, fieldName).toStringId());

                    if (fieldName.equals(FieldSet.DEF_FIELDS._size.name())) {

                        fieldItem.fieldName = "_eventStats";
                        String fieldId = new Summary(fieldName + SummaryBucket.AUTO_TAG_SUM, "", fieldName).toStringId();
                        Map map = funcResults.get(fieldId);
                        if (map == null) {
                            LOGGER.warn("Empty Stats for SIZE field");
                            continue;
                        }
                        buildEventInfoField(resultList, fieldName, fieldItem, map);
                        continue;
                    }

                    if (field.isSummary() == false || countUniqueIntMap == null) {
                        remainder.add(fieldItem);
                        fieldItem.total = "-1";
                        continue;
                    } else {
                        resultList.add(fieldItem);
                        Map countUniqueMap = funcResults.get(new CountUniqueHyperLog(fieldName + SummaryBucket.AUTO_TAG_CU, fieldName).toStringId());

                        if (countUniqueMap != null && countUniqueMap.containsKey(fieldName + SummaryBucket.AUTO_TAG_CU)) {
                            HyperLogLog object = (HyperLogLog) countUniqueMap.get(fieldName + SummaryBucket.AUTO_TAG_CU);
                            fieldItem.setTotal((int) object.cardinality());
                            if (fieldName.equals(FieldSet.DEF_FIELDS._type.name())) {
                                typeCount = (int) object.cardinality();
                            }

                        }

                        Set<String> keySet = countUniqueIntMap.keySet();
                        Integer totalCountItems = getTotal(countUniqueIntMap.values());
                        KeyValue others = new KeyValue("Other", "Other", 0, 0);
                        for (String key2 : keySet) {
                            Integer value = (Integer) countUniqueIntMap.get(key2);
                            if (value != null)
                                if (fieldItem.keyValues.size() < MAX_FIELDS_TO_DISPLAY) {
                                    fieldItem.add(new KeyValue(fieldName, key2, value, getPercent(value, totalCountItems)));
                                } else {
                                    others.percent += getPercent(value, totalCountItems);
                                    others.value += value;
                                }
                        }
                        if (others.value != 0) {
                            fieldItem.add(others);
                            others.setValue(others.value);
                        }
                        sort(fieldItem.keyValues, new Comparator<KeyValue>() {
                            public int compare(KeyValue o1, KeyValue o2) {
                                if (o1.key.equals("Other")) return 1;
                                if (o2.key.equals("Other")) return -1;
                                return Double.compare(o2.value, o1.value);
                            }
                        });
                    }

                } catch (Throwable t) {
                    System.err.println(" Failed to handle:" + field.name() + " ex:" + t);
                    t.printStackTrace();
                    LOGGER.warn("Failed:" + field.name(), t);
                }
            }

            sortAccordingToFieldSet(resultList, fieldSet);

            int maxLabelWidth = 0;
            for (FieldItem fieldItem : resultList) {
                maxLabelWidth = Math.max(fieldItem.fieldLabelWithCount.length(), maxLabelWidth);
            }
            for (FieldItem fieldItem : resultList) {
                fieldItem.adjustForWidth(maxLabelWidth);
            }

            resultList.addAll(remainder);

            if (isKeyValueExtracting) resultList.addAll(buildDynamicFields(funcResults));
            resultList.addAll(buildRemainingFields(funcResults, resultList));
            // if there is only 1 data type then add a facet call editType(

            FieldItem fieldItem = new FieldItem("_DATA_TYPE_EDITOR_=" + fieldSetId);
            fieldItem.fieldSetId = fieldSetId;
            fieldItem.funct = typeCount == 1 ? "EDITABLE:true" : "EDITABLE:false";
            resultList.add(0, fieldItem);
        } catch (Throwable t) {
            t.printStackTrace();
        }
		return resultList;
	}

	private static Collection<? extends FieldItem> buildRemainingFields(Map<String, Map> funcResults, ArrayList<FieldItem> currentResultList) {

		List<FieldItem> results = new ArrayList<FieldItem>();
		Set<String> remainingNames = getRemainingNames(funcResults, currentResultList);
		for (String fieldname : remainingNames) {
			try {
				String count = "CountFaster " + fieldname + " " + fieldname;
				String countU = "CountUniqueHyperLog " + fieldname + "_CU_ " + fieldname;
				Map countMap = funcResults.get(count);
				Map countUMap = funcResults.get(countU);
				if (countU != null && countUMap != null) {
					FieldItem fieldItem = new FieldItem(fieldname);
					HyperLogLog object = (HyperLogLog) countUMap.values().iterator().next();
					fieldItem.setTotal((int) object.cardinality());

					Set<String> keySet = countMap.keySet();
					Integer totalCountItems = getTotal(countMap.values());
					for (String key2 : keySet) {
						Integer value = (Integer) countMap.get(key2);
						if (value != null)
							fieldItem.add(new KeyValue(fieldname, key2, value, getPercent(value, totalCountItems)));
					}
					fieldItem.sortValues();
					results.add(fieldItem);
				}
			} catch (Throwable t) {
//				System.err.println(" Failed to handle:" +fieldname + " ex:" + t);
//				t.printStackTrace();
				LOGGER.warn("Failed to handle: " +fieldname + " ex:" + t);
			}
		}
		return results;
	}

	private void sortValues() {
		sort(keyValues);
	}

	private static Set<String> getRemainingNames(Map<String, Map> resultList, ArrayList<FieldItem> currentResultList) {
		HashSet<String> strings = new HashSet<String>();
		for (String funcResult : resultList.keySet()) {
			if (funcResult.startsWith("CountFaster")){// && funcResult.length() > 11) {

                String[] split = funcResult.split(" ");
                if(split.length >= 3 ) {
                    String possibleFieldd = split[2];

                    boolean found = false;
                    for (FieldItem fieldItem : currentResultList) {
                        if (fieldItem.fieldName.equals(possibleFieldd)) {
                            found = true;
                        }
                    }
                    if (!found) strings.add(possibleFieldd);
                }

			}
		}
		return strings;
	}

	private static void buildEventInfoField(ArrayList<FieldItem> resultList, String fieldName, FieldItem fieldItem, Map map) {
		SummaryStatistics stats = null;
		if (map.values().size() > 0) {
            stats = (SummaryStatistics) map.values().iterator().next();
        }


		fieldItem.add(new KeyValue(fieldName, "Total Events", stats.getN(), 0));
		fieldItem.add(new KeyValue(fieldName, "Min Size (bytes)", stats.getMin(), 0));
		fieldItem.add(new KeyValue(fieldName, "Max Size (bytes)", stats.getMax(), 0));
		fieldItem.add(new KeyValue(fieldName, "Mean Size (bytes)", (int) stats.getMean(), 0));
		double megabytes = FileUtil.getMEGABYTES(stats.getSum());
		if (megabytes < 1000) {
            fieldItem.add(new KeyValue(fieldName, "Total Volume (M)", megabytes, 0));
        } else {
            fieldItem.add(new KeyValue(fieldName, "Total Volume (G)", FileUtil.getGIGABYTES(stats.getSum()), 0));
        }
		fieldItem.funct = "sum(_tag)";
		fieldItem.setTotal( (int) stats.getMean());
		resultList.add(fieldItem);
	}

    private static List<FieldItem> buildDynamicFields(Map<String, Map> funcResults) {
        List<FieldItem> results = new ArrayList<FieldItem>();
        Set<String> discoveredKeys = getDiscoKeys(funcResults.keySet());
        for (String key : discoveredKeys) {
                try {
                    String dFieldName = key.substring(key.lastIndexOf(" ") + 1);
                    FieldItem fieldItem = new FieldItem(dFieldName);
                    fieldItem.setDynamic(true);
                    Map countMap = funcResults.get(key);
                    String dynKeyId = "DYN_" + dFieldName + SummaryBucket.AUTO_TAG_CU;
                    Map countUniqueMap = funcResults.get(new CountUniqueHyperLog(dynKeyId, dFieldName).toStringId());
                    if (countUniqueMap != null && countUniqueMap.containsKey(dynKeyId)) {
                        HyperLogLog object = (HyperLogLog) countUniqueMap.get(dynKeyId);
                        fieldItem.setTotal((int)object.cardinality());
                    }

                    KeyValue others = new KeyValue("Other", "Other", 0, 0);
                    Set<String> keySet = countMap.keySet();
                    Integer totalCountItems = getTotal(countMap.values());
                    for (String key2 : keySet) {
                        Integer value = (Integer) countMap.get(key2);
                        if (value != null)
                            if (fieldItem.keyValues.size() < MAX_FIELDS_TO_DISPLAY) {
                                fieldItem.add(new KeyValue(dFieldName, key2, value, getPercent(value, totalCountItems)));
                            } else {
                                others.percent += getPercent(value, totalCountItems);
                                others.value += value;
                            }
                    }
					fieldItem.sortValues();
                    if (others.value != 0) {
                        others.setValue(others.value);
                        fieldItem.add(others);

                    }
                    results.add(fieldItem);
                } catch (Throwable t) {
                    LOGGER.error("Key:" + key + " Failed: v:" + funcResults.get(key + SummaryBucket.AUTO_TAG_CU), t);
                }
        }
        int limit = LogProperties.getMaxFields();
        if (results.size()  > limit) {
            results = results.subList(0, limit);
        }
//        if (Boolean.getBoolean("facets.sorting.dienabled"))
        sort(results, new Comparator<FieldItem>() {
            @Override
            public int compare(FieldItem o1, FieldItem o2) {
                return o1.fieldName.toLowerCase().compareTo(o2.fieldName.toLowerCase());
            }
        });
        return results;
    }

	private static Set<String> getDiscoKeys(Set<String> fieldNames) {
		HashSet<String> results = new HashSet<String>();
		for (String key : fieldNames) {
			if (isDiscoField(key)) results.add(key);
		}
		return results;
	}

	private static boolean isDiscoField(String key) {
		return key.contains("DYN_") && !key.contains("_CU_");
	}

	private void adjustForWidth(int maxLabelWidth) {
		while (this.fieldLabelWithCount.length() < maxLabelWidth) this.fieldLabelWithCount = " " + this.fieldLabelWithCount;
	}

	private void setTotal(Integer total) {
		this.total = total.toString();//NumberFormat.getIntegerInstance().format(total);
		if (total >= CountUnique.SUMM_MAX) {
			this.fieldLabelWithCount = this.fieldName + " (" + CountUnique.SUMM_MAX + "+)";
		} else {
			this.fieldLabelWithCount = this.fieldName + " (" + this.total + ")";
		}
	}

	private static void sortAccordingToFieldSet(ArrayList<FieldItem> resultList, FieldSet fieldSet) {
		for (FieldItem fieldItem : resultList) {
			int pos = 0;
			for (FieldI field : fieldSet.fields()) {
				if (fieldItem.fieldName.equals(field.name())) {
					fieldItem.pos = pos;
				}
				pos++;
			}
		}
		sort(resultList, new Comparator<FieldItem>() {
			public int compare(FieldItem o1, FieldItem o2) {
				return Integer.valueOf(o1.pos).compareTo(o2.pos);
			}
		});
	}
	public String toString() {
		return String.format("FieldItem: %s vals:%s", fieldName, keyValues);
	}

	private static Integer getTotal(Collection<Integer> values) {
		int result = 0;
		for (Object integer : values) {
			result += getValue(integer);
		}
		return result;
	}
	private static int getPercent(Integer value, Integer max) {
		int pcValue = (int) (((double)value)/((double)max) * 100);
		if (pcValue == 0) return 1;
		return pcValue;
	}

	private static Integer getMax(Collection<Integer> values) {
		Integer result = 0;
		for (Object integer : values) {
			Integer v = getValue(integer);
			result = Math.max(result, v);
		}
		return result;
	}

	private void add(KeyValue keyValue) {
		if (keyValues == null) keyValues = new ArrayList<KeyValue>();
		keyValues.add(keyValue);
	}

	private static Integer getValue(Object integer) {
		if (integer instanceof IntValue) return ((IntValue)integer).value();
		return ((Integer)integer);
	}


    public void setDynamic(boolean dynamic) {
        this.dynamic = dynamic;
    }

}
