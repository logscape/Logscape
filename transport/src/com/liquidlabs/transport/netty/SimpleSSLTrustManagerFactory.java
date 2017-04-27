package com.liquidlabs.transport.netty;

import com.liquidlabs.transport.TransportProperties;
import org.jboss.netty.handler.ssl.util.SimpleTrustManagerFactory;
import org.jboss.netty.logging.InternalLogger;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.util.internal.EmptyArrays;

import javax.net.ssl.ManagerFactoryParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.Principal;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Created by neil on 20/05/16.
 */
public class SimpleSSLTrustManagerFactory extends SimpleTrustManagerFactory {
    private static final InternalLogger logger = InternalLoggerFactory.getInstance(SimpleSSLTrustManagerFactory.class);

    public static final TrustManagerFactory INSTANCE = new SimpleSSLTrustManagerFactory();

    private static final TrustManager tm = new X509TrustManager() {
        public void checkClientTrusted(X509Certificate[] chain, String s) {
            logger.debug("Accepting a client certificate: " + chain[0].getSubjectDN());
        }

        public void checkServerTrusted(X509Certificate[] chain, String s) throws CertificateException {
            X509Certificate x509Certificate = chain[0];
            Principal subjectDN = x509Certificate.getSubjectDN();
            if (!subjectDN.getName().equals("CN=" + TransportProperties.getSSLDomain())) {
                 throw new CertificateException("Domain not found:" + TransportProperties.getSSLDomain());
            }
        }

        public X509Certificate[] getAcceptedIssuers() {
            return EmptyArrays.EMPTY_X509_CERTIFICATES;
        }
    };

    private SimpleSSLTrustManagerFactory() { }

    @Override
    protected void engineInit(KeyStore keyStore) throws Exception { }

    @Override
    protected void engineInit(ManagerFactoryParameters managerFactoryParameters) throws Exception { }

    @Override
    protected TrustManager[] engineGetTrustManagers() {
        return new TrustManager[] { tm };
    }
}
