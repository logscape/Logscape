package com.liquidlabs.services;

import com.liquidlabs.common.HashGenerator;
import com.liquidlabs.common.collection.Arrays;
import com.liquidlabs.log.space.LogSpace;
import com.liquidlabs.vso.VSOProperties;
import com.liquidlabs.vso.deployment.DeploymentService;
import com.liquidlabs.vso.deployment.bundle.Bundle;
import com.liquidlabs.vso.deployment.bundle.BundleUnpacker;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by neil on 29/04/16.
 */
public class AutoDeployer {

    public static final String BOOT_PROPERTIES = "boot.properties";

    static final Logger LOGGER = Logger.getLogger(AutoDeployer.class);
    final AppLoader appLoader;

    public AutoDeployer(ScheduledExecutorService scheduler, DeploymentService deploymentService, LogSpace logSpace) {

        appLoader = new AppLoader(scheduler);

        final String[] appsToAutoDeploy = getAppsDeploy();
        LOGGER.info("List:[" + Arrays.toString(appsToAutoDeploy) + "]");
        File appBundleDir = new File(VSOProperties.getDeployedBundleDir());
        File systemBundleDir = new File(VSOProperties.getSystemBundleDir());


        if (appsToAutoDeploy.length > 0) {

            BundleUnpacker unpacker = new BundleUnpacker(new File(systemBundleDir.getPath()), new File(appBundleDir.getPath()));
            File[] bundleZipsToDeploy = new File("downloads").listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String filename) {
                    for (String appNameToDeploy : appsToAutoDeploy) {
                        if (filename.contains(appNameToDeploy) && filename.endsWith(".zip")) return true;
                    }
                    return false;
                }
            });
            if (bundleZipsToDeploy != null) {
                LOGGER.info("FoundCount:" + bundleZipsToDeploy.length);
                LOGGER.info("List:[" + Arrays.toString(bundleZipsToDeploy) + "]");
                for (File bundleZip : bundleZipsToDeploy) {
                    if (unpacker.isBundle(bundleZip) && !isDeployed(bundleZip.getName())){
                        Bundle bundle = unpacker.unpack(bundleZip, false);
                        HashGenerator hasher = new HashGenerator();
                        try {
                            LOGGER.info("AutoDeploy:" + bundleZip.getName());
                            String hash = hasher.createHash(bundleZip.getName(), bundleZip);
                            deploymentService.deploy(bundle.getId(), hash, false);
                            appLoader.loadConfigXMLIntoLogScape(bundle.getId(), logSpace);
                        } catch (NoSuchAlgorithmException e) {
                            e.printStackTrace();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }
                }
            }
        }
    }

    private boolean isDeployed(String bundle)  {
        bundle = bundle.replace(".zip", "");
        if (new File("deployed-bundles/" + bundle + "/" + "DEPLOYED").exists()) return true;
        if (new File("system-bundles/" + bundle + "/" + "DEPLOYED").exists()) return true;
        return false;
    }



    public String[] getAppsDeploy() {

        try {
            Properties bootProperties = getBootProperties(BOOT_PROPERTIES);
            return bootProperties.getProperty("auto-deploy", "_NONE_").split(",");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String[0];
    }
    public static Properties getBootProperties(String bootProps) throws IOException {

        ClassLoader cl = new URLClassLoader(new URL[] { new File(".").toURL() }, ClassLoader.getSystemClassLoader() );

        InputStream reader = cl.getSystemResourceAsStream(bootProps);
        if (reader == null) reader = cl.getResourceAsStream(bootProps);
        if (reader == null) {
            String property = System.getProperty("java.class.path");
            String pp = property.replaceAll(";", "\n");
            System.out.println(pp);
            System.out.println("Failed to find boot.properties on classpath:" + property);
            System.err.println("Failed to find boot.properties on classpath:" + System.getProperty("java.class.path"));
            System.exit(-1);
        }
        Properties properties = new Properties();
        properties.load(reader);
        reader.close();

        return properties;
    }

}
