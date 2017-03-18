 package com.bloom.dbsync.run;
 
 import java.util.ArrayList;
 
 public class BloomStreamStatus
 {
   public static final String STREAM_STATE_STARTED = "STARTED";
   public static final String STREAM_STATE_FAILED = "FAILED";
   public static final String STREAM_STATE_STOPPED = "STOPPED";
   public static final String STREAM_STATE_OK = "OK";
   public static final String STREAM_STATE_ERROR = "ERROR";
   private String name = "";
   private boolean isValid = false;
   private long isValidTimeStamp = 0L;
   private String state = "";
   private long stateTimeStamp = 0L;
   private boolean isSynchronized = false;
   private long isSynchronizedTimeStamp = 0L;
 
   private ArrayList<BloomModuleStatus> modulesStatusList = new ArrayList();
 
   public String getName() {
     return this.name;
   }
   public void setName(String name) {
     this.name = name;
   }
   public boolean getIsValid() {
     return this.isValid;
   }
   public void setIsValid(boolean isValid) {
     this.isValid = isValid;
     this.isValidTimeStamp = System.currentTimeMillis();
   }
   public String getState() {
     return this.state;
   }
   public void setState(String state) {
     this.state = state;
     this.stateTimeStamp = System.currentTimeMillis();
   }
   public boolean getIsSynchronized() {
     return this.isSynchronized;
   }
   public void setIsSynchronized(boolean isSynchronized) {
     this.isSynchronized = isSynchronized;
     this.isSynchronizedTimeStamp = System.currentTimeMillis();
   }
 
   public void setBloomModuleStatus(BloomModuleStatus moduleStatus)
   {
     if (this.modulesStatusList.contains(moduleStatus)) {
       this.modulesStatusList.remove(moduleStatus);
     }
 
     this.modulesStatusList.add(moduleStatus);
   }
 
   public ArrayList<BloomModuleStatus> getBloomModuleStatus()
   {
     return this.modulesStatusList;
   }
 }
