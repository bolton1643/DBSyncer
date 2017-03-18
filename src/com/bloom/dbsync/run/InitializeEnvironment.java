 package com.bloom.dbsync.run;
 
 import com.bloom.dbsync.fileLogger.LogWriter;

 import java.io.BufferedReader;
 import java.io.File;
 import java.io.InputStreamReader;
 import java.io.PrintStream;
 import org.apache.log4j.Logger;
 
 public class InitializeEnvironment
 {
   private LogWriter logger;
   private static String DBSH_ROOT_DIR;
   private static String ETC_DIR;
   private static String LOGS_DIR;
   private static String ARCH_LOGS_DIR;
   private static String UTILS_DIR;
   private static String currentVersion;
   private static File currentDir;
 
   public InitializeEnvironment()
     throws Exception
   {
     currentDir = new File(".");
     setMinimumEnviroment();
     setEnviroment();
   }
 
   public void verifyBloomVersion() {
     this.logger = new LogWriter(InitializeEnvironment.class);
     currentVersion = getBloomVersion();
     this.logger.logClass.info("**************************************************************************");
     this.logger.logClass.info("DBS-H Ltd. Big Data Integration");
     this.logger.logClass.info("Bloom version [" + currentVersion + "] Production release");
     this.logger.logClass.info("Start running");
     this.logger.logClass.info("Finished Environment Initialization");
   }
 
   private void setMinimumEnviroment() throws Exception
   {
     DBSH_ROOT_DIR = System.getenv("DBSH_ROOT_DIR");
 
     if (DBSH_ROOT_DIR == null)
     {
       File startDir = new File("..");
       DBSH_ROOT_DIR = startDir.getCanonicalPath();
     }
 
     File logsDir = new File(DBSH_ROOT_DIR + File.separator + "logs");
     if ((logsDir.exists()) && (logsDir.isDirectory()))
     {
       LOGS_DIR = logsDir.getPath();
     }
     else
     {
       LOGS_DIR = currentDir.getCanonicalPath();
       System.out.println("LOGS_DIR does not exist,logs will be generated under: " + LOGS_DIR);
     }
 
     File utilsDir = new File(DBSH_ROOT_DIR + File.separator + "utils");
     if ((utilsDir.exists()) && (utilsDir.isDirectory()))
     {
       UTILS_DIR = utilsDir.getPath();
     }
     else
     {
       UTILS_DIR = "";
     }
 
     File etcDir = new File(DBSH_ROOT_DIR + File.separator + "etc");
     if ((etcDir.exists()) && (etcDir.isDirectory()))
     {
       ETC_DIR = etcDir.getPath();
     }
     else
     {
       ETC_DIR = "";
     }
   }
 
   private void setEnviroment()
     throws Exception
   {
     String rootDirPath = DBSH_ROOT_DIR;
 
     if (LOGS_DIR.equals(currentDir.getCanonicalPath()))
     {
       throw new RuntimeException("LOGS_DIR does not exist,logs will be generated under: " + LOGS_DIR);
     }
     if (ETC_DIR.isEmpty())
     {
       throw new RuntimeException("DBS-H Error: ETC_DIR directory does not exist =" + ETC_DIR);
     }
     if (UTILS_DIR.isEmpty())
     {
       throw new RuntimeException("DBS-H Error: UTILS_DIR directory does not exist =" + UTILS_DIR);
     }
 
     File archLogsDir = new File(rootDirPath + File.separator + "archLogs");
 
     if ((archLogsDir.exists()) && (archLogsDir.isDirectory()))
     {
       ARCH_LOGS_DIR = archLogsDir.getPath();
     }
     else
     {
       throw new RuntimeException("DBS-H Error: ARCH_LOGS_DIR directory does not exist = " + archLogsDir.getPath());
     }
   }
 
   public static String getETC_DIR()
   {
     return ETC_DIR;
   }
 
   public static String getLOGS_DIR()
   {
     return LOGS_DIR;
   }
 
   public static String getARCH_LOGS_DIR()
   {
     return ARCH_LOGS_DIR;
   }
 
   public static String getUTILS_DIR()
   {
     return UTILS_DIR;
   }
 
   private String getBloomVersion()
   {
     String BloomVersion = "";
     String shellCommand = getUTILS_DIR() + File.separator + "check_Bloomjar_version.sh";
     try {
       Process p = Runtime.getRuntime().exec(shellCommand);
       p.waitFor();
       BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
       return reader.readLine();
     }
     catch (Exception ex)
     {
       this.logger.logClass.error("Error to identify Bloom version -" + ex.getMessage());
     }return BloomVersion;
   }
 }