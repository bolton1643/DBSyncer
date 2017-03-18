 package com.bloom.sync;
 
 import com.bloom.dbsync.fileLogger.LogWriter;
 
 public abstract class SyncData extends Thread
 {
   protected LogWriter logger;
   private static FullSync fullSyncObject = new FullSync();
   protected int plugNumber = 1000;
 
   public abstract void connectToSource() throws Exception;
 
   public abstract void connectToTarget() throws Exception;
 
   public abstract void getObjectsList() throws Exception;
 
   public abstract void copyData() throws Exception;
 
   public SyncData(String configurationName) { 
//	   fullSyncObject.loadFullSyncFromDB(configurationName); 
	   }
 
 
   public void updateFullSyncEstimate(String configurationName, int estimateTotalRows, int estimateSyncCompleteTimeSec)
   {
     fullSyncObject.setFullSyncConfiguration(configurationName);
     fullSyncObject.setFullSyncEstimate(estimateTotalRows, estimateSyncCompleteTimeSec);
   }
 
   public void updateFullSyncStatus(String fullSyncStatus) {
     fullSyncObject.setFullSyncProgressStatus(fullSyncStatus);
     if ("Started".equals(fullSyncStatus))
     {
       fullSyncObject.setFullSyncProgress(fullSyncStatus, 0, 0, 0);
     }
   }
 
   public static void updateFullSyncStatus(String fullSyncStatus, String configurationName)
   {
     switch (fullSyncStatus)
     {
     case "Started":
       fullSyncObject.setFullSyncProgress(fullSyncStatus, 0, 0, 0);
       break;
     case "ToStop":
       String currentStatus = fullSyncObject.getFullSyncProgressStatus();
       if (("Started".equals(currentStatus)) || ("Running".equals(currentStatus)))
       {
         fullSyncObject.setFullSyncProgressStatus(fullSyncStatus); } break;
     default:
       fullSyncObject.setFullSyncProgressStatus(fullSyncStatus);
     }
   }
 
   public void updateFullSyncProgress(int syncedRows, int syncTimeMs)
   {
     fullSyncObject.advanceFullSyncProgress("Running", syncedRows, syncTimeMs);
   }
 
   public String getfullSyncEstimate()
   {
     return fullSyncObject.getFullSyncEstimate();
   }
 
   public String getFullSyncStatus()
   {
     return fullSyncObject.getFullSyncProgressStatus();
   }
 
   protected void checkToStopFullSync() throws InterruptedException {
     String fullSyncStatus = getFullSyncStatus();
     if (("ToStop".equals(fullSyncStatus)) || ("Failed".equals(fullSyncStatus)))
     {
       throw new InterruptedException("Full Sync has been set to stop");
     }
   }
 }

