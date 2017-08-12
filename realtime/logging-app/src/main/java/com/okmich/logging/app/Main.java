/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.okmich.logging.app;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 *
 * @author m.enudi
 */
public class Main {

    /**
     *
     */
    private static final Logger logger = Logger.getLogger(Main.class);
    /**
     *
     */
    private static final java.util.logging.Logger LOG = java.util.logging.Logger.getLogger("Main");

    public static void main(String[] args) {
        String logConfigFile = System.getProperty("user.dir") + File.separator + "lib" + File.separator + "log4j.properties";
        File logProperties = new File(logConfigFile);
        PropertyConfigurator.configure(logConfigFile);
        if (!logProperties.exists()) {
            throw new IllegalArgumentException("cannot find log configuration at " + logConfigFile);
        }

        if (args.length < 1) {
            throw new IllegalArgumentException("Usage: the file to be read is required");
        }
        try {
            readAndLog(new File(args[0]));
        } catch (IOException ex) {
            LOG.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     *
     * @param arg
     * @throws IOException
     */
    private static void readAndLog(File arg) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(arg));
        String line;
        line = br.readLine();
        while (line != null) {
            logger.info(line);
            line = br.readLine();
        }
    }
}
