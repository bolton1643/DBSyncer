 package com.bloom.dbsync.run;
 
 public class BloomModuleStatus
 {
   public static final String MODULE_STATUS_ERROR = "ERROR";
   public static final String MODULE_STATUS_OK = "OK";
   private String moduleName = "";
   private String state = "";
   private long stateTimeStamp = 0L;
   private String stateInfo = "";
 
   public BloomModuleStatus(String moduleName, String state, String stateInfo)
   {
     this.moduleName = moduleName;
     this.state = state;
     this.stateInfo = stateInfo;
     this.stateTimeStamp = System.currentTimeMillis();
   }
 
   public String getModuleName() {
     return this.moduleName;
   }
 
   public void setModuleName(String moduleName) {
     this.moduleName = moduleName;
   }
 
   public String getState() {
     return this.state;
   }
 
   public void setState(String state) {
     this.state = state;
     this.stateTimeStamp = System.currentTimeMillis();
   }
 
   public String getStateInfo() {
     return this.stateInfo;
   }
 
   public void setStateInfo(String stateInfo) {
     this.stateInfo = stateInfo;
   }
 
   public boolean equals(Object o)
   {
     return ((BloomModuleStatus)o).moduleName.equals(this.moduleName);
   }
 }