package com.cloudera.example;

/**
 * Created by Ashok Rajan.
 */

import java.security.PrivilegedExceptionAction;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;

public class HdfsTest {

    public static void main(String args[]) {

        try {
            UserGroupInformation ugi
                    = UserGroupInformation.createRemoteUser("arajan");

            ugi.doAs(new PrivilegedExceptionAction<Void>() {

                public Void run() throws Exception {

                    Configuration conf = new Configuration();
                    conf.set("hadoop.security.authentication", "kerberos");
                    conf.set("fs.defaultFS", "hdfs://ec2-54-226-23-31.compute-1.amazonaws.com:8020/user/arajan");
                    conf.set("hadoop.job.ugi", "arajan");

                    FileSystem fs = FileSystem.get(conf);

                    FileStatus[] status = fs.listStatus(new Path("/user/arajan"));
                    for(int i=0;i<status.length;i++){
                        System.out.println(status[i].getPath());
                    }
                    return null;
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
