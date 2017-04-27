package com.liquidlabs.log.search;

import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.LogRequestBuilder;
import com.liquidlabs.log.search.functions.Count;
import com.liquidlabs.log.space.LogRequest;
import com.liquidlabs.transport.serialization.Convertor;
import org.joda.time.DateTime;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class BucketSerTest {
	
	DateTime now = new DateTime();
	private LogRequest request;
	private String subscriber = "sub";
	
	@Test
	public void shouldSerializeIt() throws Exception {
		request = new LogRequestBuilder().getLogRequest(subscriber, Arrays.asList("* | 0.count() verbose(true) buckets(5)" ), null, now.minusMinutes(5).getMillis(), now.getMillis());
		
		
		Bucket bucket = new Bucket(now.minusMinutes(5).getMillis(), now.minusMinutes(4).getMillis(), request.copy().queries().get(0).functions(), 0, "*", "sourceURI", subscriber, "");
		
		Count count = (Count) bucket.functions().get(0);
		count.applyCount("one", "one");
		
		Map<String, Map> functionResults = new HashMap<String, Map>();
		Map map = new HashMap<String, String>();
		map.put("result", "stufffff.log");
		functionResults.put("crap", map);
		bucket.setFunctionResults(functionResults);
		byte[] serialize = Convertor.serialize(bucket);
		Object deserialize = Convertor.deserialize(serialize);
		assertNotNull(deserialize);
		
		
		
	}

}
