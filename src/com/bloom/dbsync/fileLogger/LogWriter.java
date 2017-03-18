 package com.bloom.dbsync.fileLogger;
 
 import com.bloom.dbsync.run.InitializeEnvironment;

 import java.io.File;
 import java.util.Properties;
 import org.apache.log4j.Logger;
 import org.apache.log4j.PropertyConfigurator;
 
 public class LogWriter
 {
   public Logger logClass;
   private Properties logProperties;
 
   public LogWriter(Class className)
   {
     this.logClass = Logger.getLogger(className);
     this.logProperties = new Properties();
     File logConfigFile = new File(InitializeEnvironment.getETC_DIR() + File.separator + "log4j.properties");
     if (logConfigFile.canRead())
     {
       PropertyConfigurator.configure(InitializeEnvironment.getETC_DIR() + File.separator + "log4j.properties");
     }
     else
     {
       this.logProperties.setProperty("log4j.logger.com.dbsh", "DEBUG,TestLog");
       this.logProperties.setProperty("log4j.logger.org.apache.commons", "ERROR,TestLog");
       this.logProperties.setProperty("log4j.appender.TestLog", "org.apache.log4j.RollingFileAppender");
       this.logProperties.setProperty("log4j.appender.TestLog.File", InitializeEnvironment.getETC_DIR() + File.separator +"test.log");
       this.logProperties.setProperty("log4j.appender.TestLog.maxFileSize", "1MB");
       this.logProperties.setProperty("log4j.appender.TestLog.MaxBackupIndex", "20");
       this.logProperties.setProperty("log4j.appender.TestLog.layout", "org.apache.log4j.PatternLayout");
       this.logProperties.setProperty("log4j.appender.TestLog.layout.ConversionPattern", "%d{dd/MM/yyyy HH:mm:ss:SSS} - [%t]  %-6p %c{1} - %m%n");
       PropertyConfigurator.configure(this.logProperties);
     }
   }
 }