 package com.bloom.dbsync.utility;
 
 import com.bloom.dbsync.fileLogger.LogWriter;

 import org.apache.log4j.Logger;
 
 public class Exit
   implements Runnable
 {
   private static LogWriter logger = new LogWriter(Exit.class);
 
   public void run()
   {
     try {
       Thread.sleep(2000L);
       logger.logClass.info("The " + Thread.currentThread().getName() + " will shut down");
     } catch (InterruptedException e) {
     }
     System.exit(0);
   }
 }
