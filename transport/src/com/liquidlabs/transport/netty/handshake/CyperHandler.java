package com.liquidlabs.transport.netty.handshake;

import javax.crypto.Cipher;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;

/**
 * Created with IntelliJ IDEA.
 * User: neil
 * Date: 13/05/2014
 * Time: 12:44
 * To change this template use File | Settings | File Templates.
 */
public class CyperHandler {

    private static String keyStoreFile = System.getProperty("cert.keystore.file",System.getProperty("vscape.home","./") + "ssl/.keystore");
    public static final String keyStorePass = System.getProperty("cert.keystore.pass", "ll4bs1234");
    public static final String keyAlias = System.getProperty("cert.keystore.alias","1");
    public static final String CIPHER_MODE = "RSA/ECB/PKCS1Padding";



    public static String publicCert = System.getProperty("public.cert.file",System.getProperty("vscape.home","./") + "ssl/public.key");

    public static String privateCert = System.getProperty("private.key.file",System.getProperty("vscape.home","./") + "ssl/private.pem");

    /**
     * Encrypt using public key (client)
     * @param content
     * @return
     */
    public byte[] encrypt(byte[] content) {
        final Cipher cipher;
        try {
            cipher = Cipher.getInstance(CIPHER_MODE);
            cipher.init(Cipher.ENCRYPT_MODE, getPublicKey());
            return cipher.doFinal(content);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

    /**
     * Decrypt using private key (server)
     * @param content
     * @return
     */
    public byte[] decrypt(byte[] content) {

        final Cipher cipher;
        try {
            cipher = Cipher.getInstance(CIPHER_MODE);
            cipher.init(Cipher.DECRYPT_MODE, getPrivateKey());
            return cipher.doFinal(content);

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private Key getPrivateKey() {
        try {
            return getPrivateKey(privateCert);

        } catch (Exception e) {
            try {
                return getPrivateKeyFromStore();
            } catch (Throwable t) {
                throw new RuntimeException(t);
            }
        }

    }

    private Key getPublicKey() {
        // try and load the public.key file - otherwise fallback the the default keystore
        try {
            return getPublicKey(publicCert);
        } catch (Exception e) {
            try {
                return getPublicKeyFromStore();
            } catch (Exception t) {
                throw new RuntimeException(t);
            }
        }

    }




    public static PublicKey getPublicKeyFromStore() {

        try {
            KeyStore keystore = getKeyStore();
            KeyStore.ProtectionParameter keyPass = new KeyStore.PasswordProtection(keyStorePass.toCharArray());
            Certificate certificate = keystore.getCertificate(keyAlias);
            if (certificate == null) throw new RuntimeException("Failed to load Certificate PublicKey for Alias [invalid alias?]:" + keyAlias);
            return certificate.getPublicKey();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static PrivateKey getPrivateKeyFromStore() {
        KeyStore keystore = null;
        try {
            keystore = getKeyStore();

            KeyStore.ProtectionParameter keyPass = new KeyStore.PasswordProtection(keyStorePass.toCharArray());
            KeyStore.PrivateKeyEntry privKeyEntry = (KeyStore.PrivateKeyEntry) keystore.getEntry(keyAlias, keyPass);
            return privKeyEntry.getPrivateKey();

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }



    public static PublicKey getPublicKey(String filename)
            throws Exception {

        File f = new File(filename);
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int)f.length()];
        dis.readFully(keyBytes);
        dis.close();

        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        Certificate cert = cf.generateCertificate(new FileInputStream(filename));
        return cert.getPublicKey();
    }

    public static PrivateKey getPrivateKey(String filename) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        KeyFactory kf = KeyFactory.getInstance("RSA");

        File f = new File(filename);
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int)f.length()];
        dis.readFully(keyBytes);
        dis.close();

        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        return kf.generatePrivate(spec);

    }
    private static KeyStore getKeyStore() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        KeyStore keystore = KeyStore.getInstance("JKS");
        // fallback
        if (!new File(keyStoreFile).exists()) {
            System.err.println("Didnt file keyStore:" + new File(keyStoreFile).getAbsolutePath());
        }

        FileInputStream fileInputStream = new FileInputStream(keyStoreFile);
        keystore.load(fileInputStream, keyStorePass.toCharArray());
        fileInputStream.close();
        return keystore;
    }

}
