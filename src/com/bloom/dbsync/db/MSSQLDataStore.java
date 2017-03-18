 package com.bloom.dbsync.db;
 
 import com.bloom.dbsync.fileLogger.LogWriter;
 import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
 import java.io.PrintStream;
 import java.sql.Connection;
 import java.sql.SQLException;
 import org.apache.log4j.Logger;
 
 public class MSSQLDataStore extends DataStore
 {
   private SQLServerDataSource MSSQLDataSource;
   private Connection MSSQLConnection;
 
   public MSSQLDataStore(String dsLogicalName, String dsConnectionUrl)
     throws SQLException
   {
     super(dsLogicalName, dsConnectionUrl, "MSSQL");
 
     this.logger = new LogWriter(MSSQLDataStore.class);
     this.MSSQLDataSource = new SQLServerDataSource();
     this.MSSQLDataSource.setURL(dsConnectionUrl);
     this.MSSQLConnection = this.MSSQLDataSource.getConnection();
     this.logger.logClass.debug("connected to " + dsConnectionUrl);
   }
 
   public void printDataStoreInfo() {
     System.out.println("************** Data Store Info *******************************");
     System.out.println("Data Store type             : " + getDsType());
     System.out.println("Data Store logcial name     : " + getDsLogicalName());
     System.out.println("Data Store connection URL   : " + getDsConnectionUrl());
     System.out.println("************** Data Store Info *******************************");
   }
 
   public Connection getMSSQLConnection() {
     return this.MSSQLConnection;
   }
 
   public static String convertLsnToDecString(String hexLsn)
   {
	 if(hexLsn == null)
		 return "-1";
     long l1 = Long.parseLong(hexLsn.substring(0, 8), 16);
     long l2 = Long.parseLong(hexLsn.substring(9, 17), 16);
     long l3 = Long.parseLong(hexLsn.substring(18, hexLsn.length()), 16);
     String decLsn = String.valueOf(l1) + String.format("%010d", new Object[] { Long.valueOf(l2) }) + String.format("%05d", new Object[] { Long.valueOf(l3) });
 
     return decLsn;
   }
 
   public static String convertLsnToString(String decLsn)
   {
     String l1 = decLsn.substring(0, decLsn.length() - 15);
     String l2 = Long.toString(Long.parseLong(decLsn.substring(decLsn.length() - 15, decLsn.length() - 5)));
     String l3 = Long.toString(Long.parseLong(decLsn.substring(decLsn.length() - 5, decLsn.length())));
     String stringLsn = "'" + l1 + ":" + l2 + ":" + l3 + "'";
 
     return stringLsn;
   }
 }
