 package com.bloom.sync;
 
 import com.google.gson.Gson;
 import com.mongodb.BasicDBObject;
 import com.mongodb.DB;
 import com.mongodb.DBCollection;
 import com.mongodb.util.JSON;
 import java.text.DateFormat;
 import java.text.SimpleDateFormat;
 import java.util.Date;
 
 public class FullSync
 {
   private String configurationName = "";
   private Estimate estimate;
   private Progress progress;
 
   public FullSync()
   {
     this.configurationName = "";
     this.estimate = new Estimate();
     this.progress = new Progress();
   }
 
   public void initFullSync(String fullSyncConfiguration)
   {
     this.configurationName = fullSyncConfiguration;
     this.estimate = new Estimate();
     this.progress = new Progress();
   }
 
  
 
   public void setFullSyncProgressStatus(String fullSyncStatus)
   {
     this.progress.status = fullSyncStatus;
     DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
     this.progress.progressUpdateTime = dateFormat.format(new Date());
   }
 
   public void setFullSyncConfiguration(String setConfiguration)
   {
     this.configurationName = setConfiguration;
   }
 
   public String getFullSyncEstimate()
   {
     Gson gson = new Gson();
     String resultFullSyncEstimate = gson.toJson(this.estimate);
     return resultFullSyncEstimate;
   }
 
   public String getFullSyncProgress()
   {
     Gson gson = new Gson();
     String resultFullSyncProgress = gson.toJson(this.progress);
     return resultFullSyncProgress;
   }
 
 
   public void setFullSyncEstimate(int estimateTotalRows, int estimateSyncCompleteTimeSec)
   {
     this.estimate.totalRows = estimateTotalRows;
     this.estimate.syncCompleteTime = estimateSyncCompleteTimeSec;
     DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
     this.estimate.estimateUpdateTime = dateFormat.format(new Date());
   }
 
   public void setFullSyncProgress(String status, int rowsCompleted, int timeCompleted, int pctWorkCompleted) {
     this.progress.status = status;
     this.progress.rowsCompleted = rowsCompleted;
     this.progress.timeCompleted = timeCompleted;
     this.progress.pctWorkCompleted = pctWorkCompleted;
     DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
     this.progress.progressUpdateTime = dateFormat.format(new Date());
   }
 
   public void advanceFullSyncProgress(String status, int newSyncedRows, int newsyncTimeMs) {
     this.progress.status = status;
     this.progress.rowsCompleted += newSyncedRows;
     this.progress.timeCompleted = ((this.progress.timeCompleted * 1000.0D + newsyncTimeMs) / 1000.0D);
 
     if (this.estimate.totalRows == 0) {
       this.progress.pctWorkCompleted = 100;
     }
     else {
       this.progress.pctWorkCompleted = (this.progress.rowsCompleted * 100 / this.estimate.totalRows);
     }
     DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
     this.progress.progressUpdateTime = dateFormat.format(new Date());
   }
 
   public String getFullSyncProgressStatus() {
     return this.progress.status;
   }
 
   private class Progress
   {
     String status;
     int rowsCompleted;
     double timeCompleted;
     int pctWorkCompleted;
     String progressUpdateTime;
 
     private Progress()
     {
       this.status = "";
       this.rowsCompleted = 0;
       this.timeCompleted = 0.0D;
       this.pctWorkCompleted = 0;
       this.progressUpdateTime = "";
     }
   }
 
   private class Estimate
   {
     int totalRows;
     int syncCompleteTime;
     String estimateUpdateTime;
 
     private Estimate()
     {
       this.totalRows = 0;
       this.syncCompleteTime = 0;
       this.estimateUpdateTime = "";
     }
   }
 }
