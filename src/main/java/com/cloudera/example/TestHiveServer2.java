package com.cloudera.example;

/**
 * Created by Ashok Rajan.
 */


import java.io.IOException;
        import java.security.PrivilegedExceptionAction;
        import java.sql.*;

        import javax.security.auth.Subject;
        import javax.security.auth.callback.Callback;
        import javax.security.auth.callback.CallbackHandler;
        import javax.security.auth.callback.NameCallback;
        import javax.security.auth.callback.PasswordCallback;
        import javax.security.auth.callback.UnsupportedCallbackException;
        import javax.security.auth.login.LoginContext;
        import javax.security.auth.login.LoginException;


public class TestHiveServer2 {

//  JDBC credentials
static final String JDBC_DRIVER = "org.apache.hive.jdbc.HiveDriver";
//static final String JDBC_DB_URL = "jdbc:hive2://http://ec2-54-226-23-31.compute-1.amazonaws.com:10000/default;principal=hive/ip-10-149-136-112.ec2.internal@SECURITY.FIRESTREAMS.COM;auth=kerberos;kerberosAuthType=fromSubject;";
static final String JDBC_DB_URL = "jdbc:hive2://localhost:10000/default;principal=hive/ip-10-149-136-112.ec2.internal@SECURITY.FIRESTREAMS.COM;auth=kerberos;kerberosAuthType=fromSubject;";
static String QUERY = "SELECT name FROM default.employee";

static final String USER = null;
static final String PASS = null;

// KERBEROS Related.
static final String KERBEROS_REALM = "SECURITY.FIRESTREAMS.COM";
static final String KERBEROS_KDC = "ec2-54-226-23-31.compute-1.amazonaws.com";
static final String KERBEROS_PRINCIPAL = "arajan@SECURITY.FIRESTREAMS.COM";
static final String KERBEROS_PASSWORD = "cloudera";
//static final String jaasConfigFilePath = "/Users/arajan/Cloudera/Jayant/Cloudera-Impala-JDBC-Example/src/main/resources/login.conf";
static final String jaasConfigFilePath = "/home/ec2-user/arajan/clouderaimpala/src/main/resources/login.conf";

	/* Contents of login.conf
SampleClient {
 com.sun.security.auth.module.Krb5LoginModule required
 debug=true  debugNative=true;
};
	 */

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

    static Connection getConnection( Subject signedOnUserSubject ) throws Exception{

        Connection conn = (Connection) Subject.doAs(signedOnUserSubject, new PrivilegedExceptionAction<Object>()
        {
            public Object run()
            {
                Connection con = null;
                try {
                    Class.forName(JDBC_DRIVER);
                    con =  DriverManager.getConnection(JDBC_DB_URL,USER,PASS);
                } catch (SQLException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                return con;
            }
        });

        return conn;
    }

    // Print the result set.
    private static int  traverseResultSet(ResultSet rs, int max) throws SQLException
    {
        ResultSetMetaData metaData = rs.getMetaData();
        int rowIndex = 0;
        while (rs.next()) {
            for (int i=1; i<=metaData.getColumnCount(); i++) {
                System.out.print("  "  + rs.getString(i));
            }
            System.out.println();
            rowIndex++;
            if(max > 0 && rowIndex >= max )
                break;
        }
        return rowIndex;
    }

    private static void  traverseResultSet(ResultSet rs) throws SQLException {
        System.out.println("-- Rows from Table started ---");

        while(rs.next()){
            System.out.println(rs.getString(1));
        }
        System.out.println("-- Rows from Table finished ---");
    }

    public static void main(String[] args) {
        System.setProperty("java.security.auth.login.config", jaasConfigFilePath);
        System.setProperty("java.security.krb5.realm", KERBEROS_REALM );
        System.setProperty("java.security.krb5.kdc", KERBEROS_KDC);

        System.out.println("-- Test started ---");
        Subject sub = getSubject();

        Connection conn = null;
        try {
            conn = getConnection(sub);
            Statement stmt = conn.createStatement() ;
            ResultSet rs = stmt.executeQuery( QUERY );
            traverseResultSet(rs);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try { if (conn != null) conn.close(); } catch(Exception e) { e.printStackTrace();}
        }

        System.out.println("Test ended  ");
    }
}
