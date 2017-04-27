package com.liquidlabs.transport.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.google.common.hash.Funnels;
import com.liquidlabs.common.collection.CompactCharSequence;
import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.net.URLCodec;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.log4j.Logger;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.liquidlabs.common.ExceptionUtil;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.common.compression.CompressorConfig;
import com.liquidlabs.transport.Config;
import com.liquidlabs.transport.TransportProperties;
import com.liquidlabs.transport.proxy.ProxyClient;
import com.liquidlabs.transport.proxy.Remotable;
import com.liquidlabs.transport.proxy.events.Event;

public class Convertor {

    private static final String DOT = ".";

    private static final String ZERO_STRING = "O_STR";

    private static final String NULL = "_VS_NULL_";

    private static final String _VSCAPE_R_EX_ = "_VSCAPE_R_EX_";

    private static final String VSCAPE_REMOTE_EXCEPTION = "VScapeRemoteException";

    private final static Logger LOGGER = Logger.getLogger(Convertor.class);

    private static final String _VS_STAR_ = "_VS_STAR_";

    private static String URL_ENCODE_HEADER = "_URL_ENCODED_";
    public static final String LIST_DELIM = "^";
    public static final String OLD_LIST_DELIM = "^";
    public static final String COLLECTION_MARKER = "__VS_COLL__";

    ObjectTranslator query = new ObjectTranslator();
    public String SPLIT = Config.NETWORK_SPLIT_2;

    static String ENC_DELIM = "\\" + Config.OBJECT_DELIM;
    static String ENC_PLUS = "\\+";

    private static final String _ENC_SPLIT_ = "_ENC_&SPLT_";
    private static final String _PLUS_ = "_PL&US_";

    static String _POUND_ = "£";
    private static final String ENC_POUND_ = "_PL&POUND_";

    private static int serializationDepth = TransportProperties.getObjectGraphDepth();

    // Javas PreJDK1.6 codec is slow in a MT environment - 1.5 is our min supported jvm
    private static URLCodec urlCodec = new URLCodec("UTF-8");


    private ObjectTranslator getObjectTranslator() {
        if (this.query == null) query = new ObjectTranslator();
        return query;
    }

    public Convertor() {
    }

    public Object[] getObjectFromString(Class<?> type[], String rawValues) {
        if (type.length == 0) return new Object[0];
        String decodedString = rawValues;
        String[] values = Arrays.split(SPLIT, decodedString);
        Object[] results = new Object[type.length];

        if (values.length != type.length) {
            throw new RuntimeException(String.format("Given mismatching Type[] for value[], types[%s] values[%s]", Arrays.toString(type), Arrays.toString(values)));
        }


        int pos = 0;

        for (Class<?> class1 : type) {
            try {
                results[pos] = getObjectFromString(class1, values[pos], serializationDepth);
                pos++;
            } catch (Throwable t) {
                String msg = String.format("Cannot process item[%d] of [%d] \nClasses%s \nv[%s]", pos, type.length, java.util.Arrays.toString(type), Arrays.toString(values));
                LOGGER.warn(msg);
                throw new RuntimeException(msg, t);
            }
        }
        return results;
    }

    public <T> T getObjectFromString(Class<T> type, String value) {
        return getObjectFromString(type, value, serializationDepth);
    }

    @SuppressWarnings("unchecked")
    <T> T getObjectFromString(Class<T> type, String value, int depth) {
        if (value != null && value.equals(NULL)) return null;
        Object o = null;

        o = handleFromStringMethod(type, value);
        if (o != null) return (T) o;

        o = handleSimpleTypes(type, value);
        if (o != null) return (T) o;

        if (type.equals(Event.Type[].class)) {
            return (T) Event.Type.fromString(value);
        } else if (Object[].class.isAssignableFrom(type)) {
            Class<?> componentType = type.getComponentType();
            if (value.equals("[0]")) {
                return (T) Array.newInstance(componentType, 0);
            }
            String[] split = Arrays.split("*", value);
            Object[] result = (Object[]) Array.newInstance(componentType, split.length);
            for (int i = 0; i < split.length; i++) {
                result[i] = getObjectFromString(componentType, split[i], depth);

            }
        } else if (boolean[].class.isAssignableFrom(type)) {
            Class<?> componentType = type.getComponentType();
            if (value.equals("[0]")) {
                return (T) Array.newInstance(componentType, 0);
            }
            String[] split = Arrays.split("*", value);
            boolean[] result = (boolean[]) Array.newInstance(componentType, split.length);
            for (int i = 0; i < split.length; i++) {
                result[i] = Boolean.parseBoolean(split[i]);
            }
            return (T) result;
        } else if (type.equals(Event.Type.class)) {
            return (T) Event.Type.valueOf(value);
        } else if (type.equals(ArrayList.class) || type.equals(List.class)) {
            return (T) getListFromString(value, depth);
        } else if (type.getName().equals("java.util.Collections$SynchronizedRandomAccessList")) {
            return (T) Collections.synchronizedList(getListFromString(value, depth));
        } else if (type.equals(HashSet.class) || type.equals(Set.class)) {
            return (T) getSetFromString(new HashSet(), value, depth);
        } else if (type.equals(TreeSet.class)) {
            return (T) getSetFromString(new TreeSet(), value, depth);
        } else if (type.equals(Map.class) || type.equals(HashMap.class) || type.equals(TreeMap.class) || type.equals(LinkedHashMap.class) ||  type.equals(ConcurrentHashMap.class)) {
            return (T) getMapFromString(value);
        } else if (Remotable.class.isAssignableFrom(type)) {
            try {
                return (T) deserialize(base64Decode(value));
            } catch (IOException e) {
                throw new RuntimeException("Cannot decode:" + value, e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Classnot found decode:" + value, e);
            }
        } else if (type.equals(void.class)) {
            return null;
        } else if (value.startsWith("ClassName:")) {
            return (T) getClassFromString(value);
        } else if (depth == 0) {
            return (T) value;
        }
        return getObjectTranslator().getObjectFromFormat(type, decodeString(value), depth - 1);
    }

    private Class<?> getClassFromString(String value) {
        String className = value.substring("ClassName:".length());
        try {
            return getClass().getClassLoader().loadClass(className);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private Object handleSimpleTypes(Class type, String value) {
        Object o = null;
        if (type.equals(String.class)) {
            String decodeString = decodeString(value);
            if (decodeString.equals(ZERO_STRING)) o = "";
            else o = decodeString;
        } else if (isANumber(type)) {
            o = getNumberFromString(type, value);
        } else if (type.equals(boolean.class) || type.equals(Boolean.class)) {
            o = Boolean.parseBoolean(value);
        } else if (Enum.class.isAssignableFrom(type)) {
            o = Enum.valueOf(type, value);
        } else if (type.equals(String[].class)) {
            o = getStringArrayFromString(value);
        } else if (type.equals(Long[].class)) {
            if (value.equals("[0]")) return new Long[0];
            String[] split = Arrays.split("*", value);

            Long[] result = new Long[split.length];
            int pos = 0;
            for (String string : split) {
                result[pos++] = Long.parseLong(string);
            }
            o = result;
        } else if (type.equals(Integer[].class)) {
            if (value.equals("[0]")) return new Integer[0];
            String[] split = Arrays.split("*", value);
            Integer[] result = new Integer[split.length];
            int pos = 0;
            for (String string : split) {
                result[pos++] = Integer.parseInt(string);
            }
            o = result;

        }
        return o;
    }

    @SuppressWarnings("unchecked")
    private boolean isFromStringClass(Class type) {
        String typeName = type.getName();
        return
                typeName.equals("com.liquidlabs.transport.proxy.events.Event") ||
                        typeName.equals("com.liquidlabs.transport.proxy.Invocation") ||
                        typeName.equals("com.liquidlabs.space.map.NewState");
    }

    @SuppressWarnings("unchecked")
    private Object handleFromStringMethod(Class type, String value) {
        try {
            if (isFromStringClass(type)) {
                Method method = type.getMethod("fromString", String.class);
                Constructor<?> declaredConstructor = type.getDeclaredConstructor();
                Object newInstance = declaredConstructor.newInstance();
                method.invoke(newInstance, value);
                return newInstance;
            }
        } catch (NoSuchMethodException e1) {
        } catch (Throwable t1) {
            LOGGER.warn(t1.getMessage(), t1);
        }
        return null;
    }

    Number getNumberFromString(Class<?> type, String value) {
        if (type.equals(long.class) || type.equals(Long.class)) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException t) {
                return (long) Double.parseDouble(value);
            }
        } else if (type.equals(double.class) || type.equals(Double.class)) {
            return Double.parseDouble(value);
        } else if (type.equals(short.class) || type.equals(Short.class)) {
            return Short.parseShort(value);
        } else if (type.equals(Integer.class) || type.equals(int.class)) {
            if (value.indexOf(DOT.charAt(0)) > 0) {
                return Double.valueOf((Double.parseDouble(value))).intValue();
            }
            return Integer.parseInt(value);
        } else if (type.equals(Number.class)) {
            if (value.indexOf(DOT) > -1) {
                return Double.parseDouble(value);
            }
            return Integer.parseInt(value);
        }
        return null;

    }

    String[] getStringArrayFromString(String value) {
        String lvalue = decodeString(value);
        if (lvalue.equals("[0]")) return new String[0];
        String[] split = Arrays.split("*", lvalue);
        String[] result = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            String string2 = split[i];
            result[i] = string2.contains(_VS_STAR_) ? string2.replaceAll(_VS_STAR_, "*") : string2;
        }
        return result;
    }


    public String getStringFromObject(Object[] values, int depth) {
        StringBuilder resultBuilder = new StringBuilder();
        for (Object object : values) {
            try {
                String stringFromObject = getStringFromObject(object, depth);
                resultBuilder.append(stringFromObject);
                resultBuilder.append(SPLIT);
            } catch (Throwable t) {
                LOGGER.warn(t.toString(), t);
            }
        }
        return resultBuilder.toString();
    }

    public String getNullStringFromObject(Object[] values) {
        StringBuilder resultBuilder = new StringBuilder();
        for (Object object : values) {
            resultBuilder.append(NULL);
            resultBuilder.append(SPLIT);
        }
        return resultBuilder.toString();
    }


    public String getStringFromObject(Object value) {
        return getStringFromObject(value, serializationDepth);

    }

    @SuppressWarnings("unchecked")
    String getStringFromObject(Object value, int depth) {
        String result = "none";
        if (value == null) {
            return NULL;
        }
        Class<? extends Object> type = value.getClass();
        try {
            if (isFromStringClass(type)) {
                type.getMethod("fromString", String.class);
                return value.toString();
            }
        } catch (Throwable t) {
        }
        if (type.equals(String.class)) {
            String string = (String) value;
            if (string.length() == 0) return ZERO_STRING;

            if (string.contains(Config.OBJECT_DELIM) || string.contains(SPLIT) || string.contains("+") || string.contains(_POUND_) || string.contains("%")) {
                return URL_ENCODE_HEADER + encodeString(string);
            } else {
                return string;
            }

        } else if (value instanceof Remotable) {
            try {
                return base64EncodeToByte(serialize(value));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return Boolean.toString((Boolean) value);

        } else if (isANumber(type)) {
            return getStringFromNumber((Number) value);

        } else if (type.equals(String[].class)) {
            result = getStringFromStringArray((String[]) value);

        } else if (type.equals(boolean[].class)) {
            String stringWithDelim = Arrays.toStringWithDelim("*", (boolean[]) value);
            if (stringWithDelim.length() == 0) stringWithDelim = "[0]";
            result = stringWithDelim;


        } else if (type.equals(Long[].class) || type.equals(Integer[].class)) {
            String stringWithDelim = Arrays.toStringWithDelim("*", (Object[]) value);
            if (stringWithDelim.length() == 0) stringWithDelim = "[0]";
            result = stringWithDelim;

        } else if (type.equals(Event.Type[].class)) {
            result = Arrays.toStringWithDelim("*", (Object[]) value);

        } else if (Enum.class.isAssignableFrom(type)) {
            result = value.toString();

        } else if (Object[].class.isAssignableFrom(type)) {
            Object[] valueArray = (Object[]) value;
            StringBuilder resultBuilder = new StringBuilder();
            for (Object object : valueArray) {
                try {
                    String stringFromObject = getStringFromObject(object, depth - 1);
                    resultBuilder.append(stringFromObject).append(SPLIT);
                } catch (Throwable t) {
                    LOGGER.warn(t.getMessage(), t);
                }
            }
            result = resultBuilder.toString();

        } else if (type.equals(Map.class) || type.equals(HashMap.class) || type.equals(TreeMap.class) || type.equals(LinkedHashMap.class) || type.equals(ConcurrentHashMap.class)) {
            return getStringFromMap(value);

        } else if (type.equals(ArrayList.class) || type.equals(List.class) || type.getName().equals("java.util.Arrays$ArrayList")) {
            return getStringFromList((List<?>) value, depth);

        } else if (type.getName().equals("java.util.Collections$SynchronizedRandomAccessList")) {
            return getStringFromList((List<?>) value, depth);

        } else if (type.equals(ArrayList.class) || type.equals(List.class) || type.getName().equals("java.util.Arrays$ArrayList")) {
            return getStringFromList((List<?>) value, depth);

        } else if (type.equals(HashSet.class) || type.equals(Set.class) || type.equals(TreeSet.class)) {
            return getStringFromSet((Set<?>) value, depth);

        } else if (type.equals(Class.class)) {
            return getStringFromClass(type, (Class<?>) value);
        } else if (depth <= 0) {
            result = value.toString();

        } else if (value.toString().startsWith("proxy" + ProxyClient.DELIM)) {
            result = value.toString();

        } else {
            result = encodeString(getObjectTranslator().getStringFromObject(value, depth - 1));
        }
        return result;
    }

    private String getStringFromClass(Class<? extends Object> type, Class<?> value) {
        return String.format("ClassName:%s", value.getName());
    }

    public boolean isANumber(Class<? extends Object> type) {
        return (type.equals(Double.class) || type.equals(double.class) ||
                type.equals(Float.class) || type.equals(float.class) ||
                type.equals(Integer.class) || type.equals(int.class) ||
                type.equals(Short.class) || type.equals(short.class) ||
                type.equals(Long.class) || type.equals(long.class) || type.equals(Number.class));
    }

    public String getStringFromNumber(Number value) {
        return String.valueOf(value);
    }


//	String getStringFromMap(Object value) {
//		return new XStream().toXML(value);
//	}

    String getStringFromMap(Object value) {
        int wait = 0;
        if (value == null) return NULL;
        while (true && wait++ < 10) {
            try {
                return getSerialStringFromMap(value);
            } catch (ConcurrentModificationException ex) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
            }
        }
        throw new RuntimeException("Failed to serialize:" + value);
    }

    private String getSerialStringFromMap(Object value) {
        try {
            return base64EncodeToByte(serialize(value));
        } catch (IOException e) {
            throw new RuntimeException("cannot serialize:" + value, e);
        }
    }

    private String base64EncodeToByte(byte[] binary) {
        return new String(com.liquidlabs.common.Base64.encodeToByte(binary, true));
    }



    final public static byte[] kryoSerialize(final Object value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Output os = new Output(baos);
        Kryo kryo = getKryo();
        kryo.writeClassAndObject(os, value);
        os.close();
        return baos.toByteArray();
    }

    final public static byte[] serializeAndCompress(Object value) throws IOException {
        if (value instanceof Throwable) value = fixThrowable((Throwable) value);
        byte[] serialize = serialize(value);
        return CompressorConfig.compress(serialize);
    }

    @SuppressWarnings("unchecked")
    public Map getMapFromString(String stuff) {

        try {
            if (stuff.equals(NULL)) return null;
            byte[] binary = base64Decode(stuff);
            return (Map) deserialize(binary);
        } catch (Throwable e) {
            throw new RuntimeException(e + " STRING:" + stuff, e);
        }
    }

    public static Object deserializeAndDecompress(byte[] binary) throws IOException, ClassNotFoundException {
        byte[] decompress = CompressorConfig.decompress(binary);
        return deserialize(decompress);
    }

    final public static Object deserialize(final byte[] binary) throws IOException, ClassNotFoundException {
        return kryoDeser(binary);
    }

    final public static byte[] serialize(Object value) throws IOException {
        if (value instanceof Throwable) {
            value = fixThrowable((Throwable) value);
        }
        return kryoSerialize(value);
    }

    private static Object fixThrowable(Throwable value) {
        String result = ExceptionUtil.stringFromStack(value, -1);
        if (true) return "VScapeRemoteException:" + result;
        Field causeField;
        try {
            Throwable p = value.getCause();
            for (int i = 0; i < 10; i++) {
                if (p != null && p.getCause() != null) p = p.getCause();
            }
            causeField = Throwable.class.getDeclaredField("cause");
            causeField.setAccessible(true);
            if (p != null)causeField.set(p, null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return value;
    }

    public static Object stdDeserialize(byte[] binary) throws IOException, ClassNotFoundException {
        try {
            ByteArrayInputStream bais = new ByteArrayInputStream(binary);
            ObjectInputStream ois = new ObjectInputStream(bais);
            Object o = ois.readObject();
            ois.close();
            return o;
        } catch (IOException t) {
            try {
                String string = "Failed to deserialize bytes[]:" + binary.length + " ex:" + t.getMessage();
                throw new IOException(string);
            } catch (Throwable t2) {
                throw t;
            }
        }
    }
    public static byte[] stdSeserialize(Object binary) throws IOException, ClassNotFoundException {
        try {
            ByteArrayOutputStream bais = new ByteArrayOutputStream();
            ObjectOutputStream ois = new ObjectOutputStream(bais);
            ois.writeObject(binary);
            ois.flush();
            return bais.toByteArray();
        } catch (IOException t) {
            try {
                String string = "Failed to serialize :" + binary + " ex:" + t.getMessage();
                throw new IOException(string);
            } catch (Throwable t2) {
                throw t;
            }
        }
    }

    public static class KryoThreadLocal {
        public static final ThreadLocal threadLocal = new ThreadLocal();
        public static void set(Kryo user) {
            threadLocal.set(user);
        }
        public static void unset() {
            threadLocal.remove();
        }
        public static Kryo get() {
            return (Kryo) threadLocal.get();
        }
    }

    public static Object kryoDeser(byte[] binary) {
        if (binary == null) return null;
        ByteArrayInputStream iss = new ByteArrayInputStream(binary);
        Kryo kryo = getKryo();
        return kryo.readClassAndObject(new Input(iss));
    }


    private static Kryo getKryo() {
        Kryo kryo = KryoThreadLocal.get();
        if (kryo != null) return kryo;
        kryo = new Kryo();
        kryo.setReferences(false);
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        kryo.register(CompactCharSequence.class, new CompactCharSequence.Serializer());
        kryo.register(TObjectIntHashMap.class, new TObjectIntHashMapSerializer());

        kryo.addDefaultSerializer(java.util.Arrays.asList().getClass(), new ArraysAsListSerializer());
        kryo.addDefaultSerializer(com.google.common.hash.Funnel.class, new StringCharsetFunnelSerializer());


        KryoThreadLocal.set(kryo);
        return kryo;
    }
    public static class TObjectIntHashMapSerializer extends Serializer<Object> {

        @Override
        public void write(final Kryo _kryo, Output output, Object obj) {
            _kryo.writeClass(output, obj.getClass());
            new JavaSerializer().write(_kryo, output, obj);
        }

        @Override
        public Object read(Kryo _kryo, Input input, Class<Object> aClass) {
            Registration inreg = _kryo.readClass(input);
            return new JavaSerializer().read(_kryo, input, aClass);
        }
    }
    public static class StringCharsetFunnelSerializer  extends Serializer<Object>  {
        @Override
        public Object read(Kryo _kryo, Input input, Class<Object> clazz) {
            Registration inreg = _kryo.readClass(input);
            String s = _kryo.readObject(input, String.class);
            return Funnels.stringFunnel(Charset.defaultCharset());
        }
        public void write(Kryo _kryo, Output output, Object obj) {
            _kryo.writeClass(output, obj.getClass());
            _kryo.writeObject( output, obj.toString() );
        }
    }

    private byte[] base64Decode(String stuff) {
        return com.liquidlabs.common.Base64.decodeFast(stuff);
    }


    String getStringFromStringArray(String[] value) {
        String[] escapedString = new String[value.length];
        for (int i = 0; i < value.length; i++) {
            String valueString = value[i];
            escapedString[i] = valueString.contains("*") ? valueString.replaceAll("\\*", _VS_STAR_) : valueString;
        }

        String stringWithDelim = Arrays.toStringWithDelim("*", escapedString);
        if (stringWithDelim.length() == 0) stringWithDelim = "[0]";
        return encodeString(stringWithDelim);
    }

    @SuppressWarnings("unchecked")
    List getListFromString(String value, int depth) {
        try {
            if (value.equals("EMPTY_LIST")) return new ArrayList();

            String decodeString = decodeString(value);
            return decodeListOrSet(decodeString, depth);

        } catch (ClassNotFoundException t) {
            String msg = "Failed to decode List from StringValue[" + value + "] depth:" + depth + ":" + t.getMessage();
            LOGGER.error(msg);
            throw new RuntimeException(msg);
        }
    }

    private ArrayList getCollectionOld(String value, int depth) throws ClassNotFoundException {
        ArrayList result = new ArrayList();

        String[] values = Arrays.split("^", value);
        for (String valueString : values) {
            String[] classAndString = Arrays.split(Config.OBJECT_DELIM, valueString);
            Class<?> classToCreate = Class.forName(classAndString[0]);
            try {
                Object stringFromObject = getObjectTranslator().getObjectFromFormat(classToCreate, valueString, depth - 1);

                if (stringFromObject.toString().startsWith(URL_ENCODE_HEADER)) stringFromObject = decodeString((String) stringFromObject);

                if (stringFromObject.toString().contains(_VSCAPE_R_EX_)) stringFromObject = stringFromObject.toString().replaceAll(_VSCAPE_R_EX_, VSCAPE_REMOTE_EXCEPTION);
                result.add(stringFromObject);
            } catch (Throwable t) {
                LOGGER.warn(String.format("Ignoring, Cannot decode ListItem[%s]",valueString), t);

            }
        }
        return result;
    }



    private ArrayList decodeListOrSet(String decodeString, int depth) throws ClassNotFoundException {
        if (!decodeString.startsWith(COLLECTION_MARKER)) {
            return getCollectionOld(decodeString, depth);
        }

        String listString = decodeString.substring(COLLECTION_MARKER.length());
        ArrayList result = new ArrayList();
        int split = listString.indexOf(":");
        int last = 0;
        while (split > -1) {
            int strLen = Integer.valueOf(listString.substring(last, split));
            int start = split + 1;
            int end = start + strLen;
            String valueString = listString.substring(start, end);
            String[] classAndString = Arrays.split(Config.OBJECT_DELIM, valueString);
            Class<?> classToCreate = Class.forName(classAndString[0]);
            try {
                Object stringFromObject = getObjectTranslator().getObjectFromFormat(classToCreate, valueString, depth - 1);

                if (stringFromObject.toString().startsWith(URL_ENCODE_HEADER))
                    stringFromObject = decodeString((String) stringFromObject);

                if (stringFromObject.toString().contains(_VSCAPE_R_EX_))
                    stringFromObject = stringFromObject.toString().replaceAll(_VSCAPE_R_EX_, VSCAPE_REMOTE_EXCEPTION);
                result.add(stringFromObject);
            } catch (Throwable t) {
                LOGGER.warn(String.format("Ignoring, Cannot decode ListItem[%s]", valueString), t);

            }
            last = end;
            split = listString.indexOf(":", last);
        }
        return result;
    }

    String getStringFromList(List<?> value, int depth) {
        while (true) {
            try {
                return getStringFromListINTERNAL(value, depth);
            } catch (ConcurrentModificationException ex) {
                pause();
            }
        }
    }

    private String getStringFromListINTERNAL(List<?> value, int depth) {
        List<?> values = (List<?>) value;
        StringBuilder resultBuilder = new StringBuilder();
        if (values.size() == 0) return "EMPTY_LIST";

        return encodeCollection(depth, values, resultBuilder);
    }

    private String encodeCollection(int depth, Collection values, StringBuilder resultBuilder) {
        resultBuilder.append(COLLECTION_MARKER);
        for (Object object : values) {
            try {
                if (object == null) continue;

                // need to encode string contents
                if (object instanceof String) {
                    object = URL_ENCODE_HEADER + encodeString((String) object);
                }
                String stringFromObject = getObjectTranslator().getStringFromObject(object, depth - 1);

                if (stringFromObject.contains(VSCAPE_REMOTE_EXCEPTION)) {
                    stringFromObject = stringFromObject.replace(VSCAPE_REMOTE_EXCEPTION, _VSCAPE_R_EX_);
                }
                int length = stringFromObject.length();
                resultBuilder.append(length).append(":").append(stringFromObject);
            } catch (Throwable t) {
                LOGGER.warn(t.getMessage(), t);
            }
        }
        return encodeString(resultBuilder.toString());
    }

    private String getStringFromSetINTERNAL(Set<?> value, int depth) {
        Set<?> values = (Set<?>) value;
        StringBuilder resultBuilder = new StringBuilder();
        if (values.size() == 0) return "";

        return encodeCollection(depth, value, resultBuilder);

    }



    @SuppressWarnings("unchecked")
    Set getSetFromString(Set result, String value, int depth) {
        if (value == null || value.trim().length() == 0) return result;
        try {
            String decodeString = decodeString(value);
            result.addAll(decodeListOrSet(decodeString, depth));
            return result;
        } catch (ClassNotFoundException e) {
            String msg = "Failed to decode Set from StringValue[" + value + "] depth:" + depth + ":" + e.getMessage();
            LOGGER.error(msg);
            throw new RuntimeException(msg);
        }
    }

    String getStringFromSet(Set<?> value, int depth) {
        while (true) {
            try {
                return getStringFromSetINTERNAL(value, depth);
            } catch (ConcurrentModificationException ex) {
                pause();
            }
        }
    }

    private void pause() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
        }
    }


    public String decodeString(String value) {
        try {
            String result = value;
            if (value.startsWith(URL_ENCODE_HEADER)) {
                result = urlCodec.decode(value.substring(URL_ENCODE_HEADER.length()), "UTF-8");
            } else {
                result = urlCodec.decode(value, "UTF-8");
            }
            return fixEscapes(result);
        } catch (UnsupportedEncodingException e1) {
            throw new RuntimeException(e1);
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    private String fixEscapes(String result) {
        result = result.contains(_ENC_SPLIT_) ? result.replaceAll(_ENC_SPLIT_, Config.OBJECT_DELIM) : result;
        if (result.contains(ENC_POUND_)) result = result.replaceAll(ENC_POUND_, _POUND_);
        return result.contains(_PLUS_) ? result.replaceAll(_PLUS_, "+") : result;
    }


    public String encodeString(String value) {
        try {
            String replacedValue = escapeString(value);
            return urlCodec.encode(replacedValue, "UTF-8");
        } catch (UnsupportedEncodingException e1) {
            throw new RuntimeException(e1);
        }
    }

    private String escapeString(String value) {
        String replacedValue = value.contains(Config.OBJECT_DELIM) ? value.replaceAll(ENC_DELIM, _ENC_SPLIT_) : value;
        replacedValue = value.contains("+") ? value.replaceAll(ENC_PLUS, _PLUS_) : value;
        if (replacedValue.contains(_POUND_)) replacedValue = replacedValue.replaceAll(_POUND_, ENC_POUND_);
        return replacedValue;
    }

    public static Object clone(Object event) {
        try {
            return deserialize(serialize(event));
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to SER:",e);
        }
    }


}

