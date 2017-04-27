package com.liquidlabs.transport.serialization;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class JdkProxySerializer  extends Serializer<Object> {


    public JdkProxySerializer() {
    }


    @Override
    public Object read(Kryo _kryo, Input input, Class<Object> clazz) {

        final InvocationHandler invocationHandler = (InvocationHandler) _kryo.readClassAndObject( input );

        final Class<?>[] interfaces = _kryo.readObject( input, Class[].class );

        final ClassLoader classLoader = _kryo.getClassLoader();

        try {
            return Proxy.newProxyInstance( classLoader, interfaces, invocationHandler );

        } catch( final RuntimeException e ) {
        	e.printStackTrace();
            System.err.println( getClass().getName()+ ".read:\n" +

            		"Could not create proxy using classLoader " + classLoader + "," +

            		" have invoctaionhandler.classloader: " + invocationHandler.getClass().getClassLoader() +

                    " have contextclassloader: " + Thread.currentThread().getContextClassLoader() );

            throw e;
        }
    }

    public void write(Kryo _kryo, Output output, Object obj) {

        InvocationHandler invocationHandler = Proxy.getInvocationHandler( obj );
		_kryo.writeClassAndObject(output, invocationHandler );

        _kryo.writeObject( output, obj.getClass().getInterfaces() );

    }

}
