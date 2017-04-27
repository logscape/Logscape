package com.liquidlabs.log.indexer;

import com.liquidlabs.log.index.Bucket;
import com.liquidlabs.log.index.BucketKey;
import com.liquidlabs.log.index.Line;

import java.util.List;

/**
 * Created by neil on 06/08/2015.
 */
public interface LineStore {
    void add(List<Line> lines);
}
