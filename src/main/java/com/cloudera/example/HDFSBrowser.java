package com.cloudera.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.PrivilegedExceptionAction;
import java.sql.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import javax.security.auth.Subject;
import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

/**
 * Created by Ashok Rajan.
 */
public class HDFSBrowser {
    static final String jaasConfigFilePath = "/home/ec2-user/arajan/clouderaimpala/src/main/resources/login.conf";
    static final String KERBEROS_REALM = "SECURITY.FIRESTREAMS.COM";
    static final String KERBEROS_KDC = "ec2-54-226-23-31.compute-1.amazonaws.com";


    static final String KERBEROS_PRINCIPAL = "arajan@SECURITY.FIRESTREAMS.COM";
    static final String KERBEROS_PASSWORD = "cloudera";


    public void listFolder(String folderPath) throws IOException {

        System.out.println("Folder to list "+folderPath);
        Configuration conf = new Configuration();

        // Conf object will read the HDFS configuration parameters from these
        // XML files.
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        Path path = new Path(folderPath);
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] files = fileSystem.globStatus(path);
        System.out.println("Listing "+folderPath);
        for (FileStatus file : files ){
            System.out.println(file.getPath().getName());
        }

    }

    public static class MyCallbackHandler implements CallbackHandler {

        public void handle(Callback[] callbacks)
                throws IOException, UnsupportedCallbackException {
            for (int i = 0; i < callbacks.length; i++) {
                if (callbacks[i] instanceof NameCallback) {
                    NameCallback nc = (NameCallback)callbacks[i];
                    nc.setName(KERBEROS_PRINCIPAL);
                } else if (callbacks[i] instanceof PasswordCallback) {
                    PasswordCallback pc = (PasswordCallback)callbacks[i];
                    pc.setPassword(KERBEROS_PASSWORD.toCharArray());
                } else throw new UnsupportedCallbackException
                        (callbacks[i], "Unrecognised callback");
            }
        }
    }

    static Subject getSubject() {
        Subject signedOnUserSubject = null;

        // create a LoginContext based on the entry in the login.conf file
        LoginContext lc;
        try {
            lc = new LoginContext("SampleClient", new MyCallbackHandler());
            // login (effectively populating the Subject)
            lc.login();
            // get the Subject that represents the signed-on user
            signedOnUserSubject = lc.getSubject();
        } catch (LoginException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
            System.exit(0);
        }
        return signedOnUserSubject;
    }

    static FileStatus[] getFiles(Subject signedOnUserSubject,final String folderPath ) throws Exception {

        FileStatus[] files = (FileStatus[]) Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Object>()
        {
            public Object run()
            {
                System.out.println("Folder to list "+folderPath);
                Configuration conf = new Configuration();

                // Conf object will read the HDFS configuration parameters from these
                // XML files.
                conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
                conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
                Path path = new Path(folderPath);
                FileStatus[] files =null;

                try{
                    FileSystem fileSystem = FileSystem.get(conf);
                    files = fileSystem.globStatus(path);
                }catch(IOException ioe){
                    ioe.printStackTrace();
                }

                return files;
            }
        });

        return files;
    }

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: HDFSBrowser list <hdfs_path>");
            System.exit(1);
        }

        HDFSBrowser browser = new HDFSBrowser();

        System.setProperty("java.security.auth.login.config", jaasConfigFilePath);
        System.setProperty("java.security.krb5.realm", KERBEROS_REALM );
        System.setProperty("java.security.krb5.kdc", KERBEROS_KDC);

        if (args[0].equals("list")) {

            if (args.length < 2) {
                System.out.println("Usage: HDFSBrowser list <hdfs_path>");
                System.exit(1);
            }
            System.out.println("browser.listFolder "+args[1]);

            Subject sub = getSubject();

            try {
                FileStatus[] files = getFiles(sub,args[1]);
                System.out.println("Listing "+args[1]);
                for (FileStatus file : files ){
                    System.out.println(file.getPath().getName());
                }
            } catch (Exception e){
                e.printStackTrace();
            } finally {
            }
        }





        System.out.println("Done!");
    }

}
