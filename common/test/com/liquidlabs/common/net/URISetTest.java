package com.liquidlabs.common.net;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;
import org.objenesis.strategy.StdInstantiatorStrategy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class URISetTest {
	
	
	@Test
	public void shouldSerializeIt() throws Exception {
		URI uri = new URI("stcp://localhost:1111/stuff");
		byte[] bytes = kryoSerialize(uri);
		URI kryoDeser = (URI) kryoDeser(bytes);
		assertEquals(1111, kryoDeser.getPort());
	}
	
	 public static Object kryoDeser(byte[] binary) {
	    	ByteArrayInputStream iss = new ByteArrayInputStream(binary);
	    	Kryo kryo = getKryo();
			return kryo.readClassAndObject(new Input(iss));
	    }
	  final public static byte[] kryoSerialize(final Object value) throws IOException {
	    	ByteArrayOutputStream baos = new ByteArrayOutputStream();
			Output os = new Output(baos);
			Kryo kryo = getKryo();
			kryo.writeClassAndObject(os, value);
			os.close();
	    	return baos.toByteArray();
	    }
	  private static Kryo getKryo() {
			Kryo kryo = new Kryo();
		  	kryo.register(URI.class);
	    	kryo.setReferences(false);
	    	kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
			return kryo;
		}


}
