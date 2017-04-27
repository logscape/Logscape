package com.liquidlabs.transport.serialization;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Neiil
 * Date: 22/08/14
 * Time: 19:45
 * To change this template use File | Settings | File Templates.
 */
public class MyMapSerializer extends MapSerializer {

    protected Map create (Kryo kryo, Input input, Class<Map> type) {
        return new HashMap();
    }

}
