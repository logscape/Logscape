package com.liquidlabs.transport.netty.handshake;

import sun.misc.BASE64Encoder;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import java.io.*;
import java.security.*;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Enumeration;

/**
 *
 * Generate keystore
 * keytool -genkey -alias certificatekey -keyalg RSA -validity 7 -keystore keystore.jks
 *
 * List contents of jks
 * keytool -list -v -keystore keystore.jks
 *
 * Export the public key
 * keytool -export -alias certificatekey -keystore keystore.jks -rfc -file public.cert
 * keytool -export -alias 1 -v -keystore .keystore -rfc -file public.key
 *
 *
 * Export the private key (more steps)
 * 1.
 * keytool -v -importkeystore -srckeystore .keystore -srcalias 1 -destkeystore myp12file.p12 -deststoretype PKCS12
 *
 * 2.
 * openssl pkcs12 -in myp12file.p12 -out private.pem
 *
 * 3.
 * openssl pkcs8 -topk8 -inform PEM -outform DER -in  private.pem -out private.der -nocrypt
 *
 * See http://crishantha.com/wp/?p=445
 *
 */


public class CryptoSample {
    //keystore related constants
    private static String keyStoreFile = "dashboardServer/ssl/.keystore";
    private static String publicKey = "dashboardServer/ssl/public.key";
    private static String privateKeyFile = "dashboardServer/ssl/private.der";
    private static String password = "ll4bs1234";
    private static String alias = "1";

    public static void main(String[] args) {

        try {
            KeyStore keystore = KeyStore.getInstance("JKS");
            char[] storePass = password.toCharArray();

            //load the key store from file system
            FileInputStream fileInputStream = new FileInputStream(keyStoreFile);
            keystore.load(fileInputStream, storePass);
            fileInputStream.close();

            /***************************signing********************************/
            //read the private key
            KeyStore.ProtectionParameter keyPass = new KeyStore.PasswordProtection(storePass);
            Enumeration<String> aliases = keystore.aliases();
            while (aliases.hasMoreElements()) {
                String aliasess = aliases.nextElement();
                System.out.println("A:"+ aliasess);

            }


            PublicKey publicKey1 = getPublicKey(publicKey);
            System.out.println("Public Key:" + publicKey1);


            KeyStore.PrivateKeyEntry privKeyEntry = (KeyStore.PrivateKeyEntry) keystore.getEntry(alias, keyPass);
            PrivateKey privateKey = getPrivateKey(privateKeyFile);// privKeyEntry.getPrivateKey();


//            //initialize the signature with signature algorithm and private key
//            Signature signature = Signature.getInstance("SHA256withRSA");
//            signature.initSign(privateKey);

            //Must be less that 53 byres
            String data = "{\n" +
                    "  \"userName\":\"neil\",\n" +
                    "  \"password\":\"crap\",\n" +
                    "  }\n" +
                    "}";

            byte[] dataInBytes = data.getBytes();

//            //update signature with data to be signed
//            signature.update(dataInBytes);
//
//            //sign the data
//            byte[] signedInfo = signature.sign();
            String breaker = "\n======================================\n";
//
//            System.out.println("Signature:" +  breaker + new BASE64Encoder().encode(signedInfo) + breaker);

            /**
             * Now apply a cypher
             */
            final Cipher cipher = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            cipher.init(Cipher.ENCRYPT_MODE, publicKey1);
            byte[] encrypted = cipher.doFinal(dataInBytes);

            System.out.println("Encrypted:" + breaker +   new BASE64Encoder().encode(encrypted) + breaker);


            /**************************verify the cypher****************************/

            final Cipher cipherIn = Cipher.getInstance("RSA/ECB/PKCS1Padding");
            cipher.init(Cipher.DECRYPT_MODE, privateKey);
            byte[] bytes = cipher.doFinal(encrypted);

            System.out.println("Got:" + new String(bytes));

            assert new String(bytes).equals(data);


            //create signature instance with signature algorithm and public cert, to verify the signature.
//            Signature verifySig = Signature.getInstance("SHA256withRSA");
//            verifySig.initVerify(privateKey);

            //update signature with signature data.
            //verifySig.update(dataInBytes);

//            //verify signature
//            boolean isVerified = verifySig.verify(signedInfo);
//
//            if (isVerified) {
//                System.out.println("Signature verified successfully");
//            }

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (KeyStoreException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (CertificateException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (UnrecoverableKeyException e) {
            e.printStackTrace();
        } catch (UnrecoverableEntryException e) {
            e.printStackTrace();
        } catch (InvalidKeyException e) {
            e.printStackTrace();
        } catch (SignatureException e) {
            e.printStackTrace();
        } catch (NoSuchPaddingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (BadPaddingException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalBlockSizeException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (Exception e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
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

    /**
     * # generate a 2048-bit RSA private key
     $ openssl genrsa -out private_key.pem 2048

     # convert private Key to PKCS#8 format (so Java can read it)
     $ openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem \
     -out private_key.der -nocrypt

     # output public key portion in DER format (so Java can read it)
     $ openssl rsa -in private_key.pem -pubout -outform DER -out public_key.der
     You keep private_key.pem around for reference, but you hand the DER versions to your Java programs.
     */
    public static PrivateKey getPrivateKey(String filename) throws NoSuchAlgorithmException, IOException, InvalidKeySpecException {
        KeyFactory kf = KeyFactory.getInstance("RSA");
// Read privateKeyDerByteArray from DER file.

        File f = new File(filename);
        FileInputStream fis = new FileInputStream(f);
        DataInputStream dis = new DataInputStream(fis);
        byte[] keyBytes = new byte[(int)f.length()];
        dis.readFully(keyBytes);
        dis.close();

        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        return kf.generatePrivate(spec);

    }
}

