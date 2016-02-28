package com.cloudera.example;

/**
 * Created by Ashok Rajan.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;


public class HDFSKerberosClient {

    static final String jaasConfigFilePath = "/home/ec2-user/arajan/clouderaimpala/src/main/resources/login.conf";
    static final String KERBEROS_REALM = "SECURITY.FIRESTREAMS.COM";
    static final String KERBEROS_KDC = "ec2-54-226-23-31.compute-1.amazonaws.com";

    public static void main(String[] args) {

        //System.setProperty("java.security.krb5.conf", );
        System.setProperty("java.security.auth.login.config", jaasConfigFilePath);
        System.setProperty("java.security.krb5.realm", KERBEROS_REALM );
        System.setProperty("java.security.krb5.kdc", KERBEROS_KDC);


        Configuration conf = new Configuration();
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));

        conf.set("hadoop.security.authentication", "kerberos");
//        conf.set("fs.defaultFS", "webhdfs://10.31.251.254:50070");
//        conf.set("fs.webhdfs.impl", org.apache.hadoop.hdfs.web.WebHdfsFileSystem.class.getName());
//        conf.set("com.sun.security.auth.module.Krb5LoginModule", "required");
//        conf.set("debug", "true");
//        conf.set("ticketCache", "DIR:/etc/");
        System.out.print("Conf......");

        UserGroupInformation.setConfiguration(conf);

        try {
            UserGroupInformation.loginUserFromKeytab("arajan@SECURITY.FIRESTREAMS.COM", "/home/ec2-user/arajan/arajan.keytab");


        Path path = new Path(args[0]);
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] files = fileSystem.globStatus(path);
        System.out.println("Listing "+args[0]);
        for (FileStatus file : files ){
            System.out.println(file.getPath().getName());
        }

        } catch (IOException ioe) {
            ioe.printStackTrace();
        }


//        System.out.print("Obtained......");
//        URI uri = URI.create("webhdfs://Dummy:50070");
//        FileSystem fs = FileSystem.get(uri, conf);


    }
}
