 package com.bloom.dbsync.db;
 
 import com.bloom.dbsync.fileLogger.LogWriter;
 
 public abstract class DataStore
 {
   protected String dsLogicalName;
   protected String dsConnectionUrl;
   protected String dsType;
   protected LogWriter logger;
 
   protected DataStore(String dsLogicalName, String dsConnectionUrl, String dsType)
   {
     this.dsConnectionUrl = dsConnectionUrl;
     this.dsLogicalName = dsLogicalName;
     this.dsType = dsType;
   }
   protected DataStore() {
   }
 
   public String getDsLogicalName() {
     return this.dsLogicalName;
   }
 
   public String getDsConnectionUrl() {
     return this.dsConnectionUrl;
   }
 
   public String getDsType() {
     return this.dsType;
   }
 
   public abstract void printDataStoreInfo();
 }
