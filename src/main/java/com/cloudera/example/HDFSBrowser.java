package com.cloudera.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

/**
 * Created by Ashok Rajan.
 */
public class HDFSBrowser {

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

    public static void main(String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("Usage: HDFSBrowser list <hdfs_path>");
            System.exit(1);
        }

        HDFSBrowser browser = new HDFSBrowser();

        if (args[0].equals("list")) {

            if (args.length < 2) {
                System.out.println("Usage: HDFSBrowser list <hdfs_path>");
                System.exit(1);
            }
            System.out.println("browser.listFolder "+args[1]);

            browser.listFolder(args[1]);
        }

        System.out.println("Done!");
    }

}
