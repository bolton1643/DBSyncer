 package com.bloom.dbsync.logreader;
 
 import com.bloom.dbsync.fileLogger.LogWriter;

 import java.io.File;
 import java.io.FileInputStream;
 import java.io.FileOutputStream;
 import java.io.IOException;
 import java.io.RandomAccessFile;
 import java.nio.charset.Charset;
 import java.nio.file.CopyOption;
 import java.nio.file.Files;
 import java.nio.file.LinkOption;
 import java.nio.file.Path;
 import java.nio.file.Paths;
 import java.nio.file.StandardCopyOption;
 import java.util.zip.GZIPInputStream;
 import org.apache.log4j.Logger;
 
 public class LogFile extends File
 {
   private Path filePath;
   private Charset charset;
   protected LogWriter logger;
 
   public LogFile(String StrfilePath)
   {
     super(StrfilePath);
     this.filePath = Paths.get(StrfilePath, new String[0]);
     this.charset = Charset.forName("UTF-8");
     this.logger = new LogWriter(LogFile.class);
   }
 
   public long getFileSize() {
     try {
       return Files.size(this.filePath);
     }
     catch (IOException ioex) {
       this.logger.logClass.error(ioex.getMessage());
     }return -1L;
   }
 
   public Path getFileName()
   {
     return this.filePath.getFileName();
   }
 
   public Path getFileDir() {
     return this.filePath.getParent();
   }
 
   public String getFullFilePath() {
     return this.filePath.toAbsolutePath().toString();
   }
 
   public void fileExists() {
     try {
       if (!Files.exists(this.filePath, new LinkOption[0]))
         throw new ParserExceptions(10);
     }
     catch (ParserExceptions pe) {
       this.logger.logClass.error(pe.getMessage());
     }
   }
 
   public void moveFile(String newName)
   {
     Path targetPath = Paths.get(newName, new String[0]);
     try
     {
       this.filePath = Files.move(this.filePath, targetPath, new CopyOption[] { StandardCopyOption.REPLACE_EXISTING });
     }
     catch (IOException ioex) {
       this.logger.logClass.error("I/O Error : [" + ioex.getMessage() + "] - Failed to move file from " + this.filePath.toString() + " to " + targetPath.toString());
     }
   }
 
   public boolean sendLogFile()
   {
     return true;
   }
 
   public long zipLogFile()
   {
     return getFileSize();
   }
 
   public boolean encryptLogFile() {
     return true;
   }
 
   public boolean isOpen()
   {
     try {
       String lsofCmd = "lsof " + getFullFilePath();
 
       Process chkPid = Runtime.getRuntime().exec(lsofCmd);
       int returnCode = chkPid.waitFor();
       if (returnCode == 0)
       {
         return true;
       }
 
       return false;
     }
     catch (IOException ioex)
     {
       this.logger.logClass.error("I/O Error : [" + ioex.getMessage() + "] -Failed to check if " + getFullFilePath() + " is opened");
       return true;
     }
     catch (InterruptedException interEx) {
       this.logger.logClass.error("Interrupt Exception to wait for lsof [" + interEx.getMessage() + "] -Failed to check if " + getFullFilePath() + " is opened");
     }return true;
   }
 
   public LogFile[] listDir()
   {
     LogFile[] lgList;
     if (this.filePath.toFile().isDirectory()) {
       File[] fList = listFiles();
       	  lgList = new LogFile[fList.length];
       for (int i = 0; i < fList.length; i++) {
         lgList[i] = new LogFile(fList[i].getAbsolutePath());
       }
     }
     else
     {
       lgList = new LogFile[1];
       lgList[0] = new LogFile(this.filePath.toFile().getAbsolutePath());
     }
     return lgList;
   }
 
   public boolean checkIfZipped()
   {
     String fileToCheck = getFullFilePath();
     try { RandomAccessFile in = new RandomAccessFile(fileToCheck, "r"); Throwable localThrowable2 = null;
       try {
         int controlMagicNumber = in.read() & 0xFF | in.read() << 8 & 0xFF00;
         boolean bool;
         if (controlMagicNumber != 35615)
         {
           return false;
         }
 
         return true;
       }
       catch (Throwable localThrowable1)
       {
         localThrowable2 = localThrowable1; throw localThrowable1;
       }
       finally
       {
         if (in != null) if (localThrowable2 != null) try { in.close(); } catch (Throwable x2) { localThrowable2.addSuppressed(x2); } else in.close();  
       } } catch (IOException ioEx) {
       this.logger.logClass.error("I/O Error : [" + ioEx.getMessage() + "]");
     }return false;
   }
 
   public void gunzipFile() {
     String fileToCheck = getFullFilePath();
     String outFileName = fileToCheck.replaceAll(".gz", "");
     int gunzippedExpectedFileSize = 0;
     try {
       FileInputStream inFile = new FileInputStream(fileToCheck);
 
       Throwable localThrowable5 = null;
       try {
         RandomAccessFile raf = new RandomAccessFile(fileToCheck, "r");
 
         Throwable localThrowable6 = null;
         try
         {
           GZIPInputStream gzipInFile = new GZIPInputStream(inFile);
 
           Throwable localThrowable7 = null;
           try
           {
             FileOutputStream outFile = new FileOutputStream(outFileName);
 
             Throwable localThrowable8 = null;
             try
             {
               raf.seek(raf.length() - 4L);
               int b4 = raf.read();
               int b3 = raf.read();
               int b2 = raf.read();
               int b1 = raf.read();
               gunzippedExpectedFileSize = b1 << 24 | (b2 << 16) + (b3 << 8) + b4;
               byte[] buffer = new byte[1024];
               int len;
               while ((len = gzipInFile.read(buffer)) != -1)
                 outFile.write(buffer, 0, len);
             }
             catch (Throwable localThrowable1)
             {
               localThrowable8 = localThrowable1; throw localThrowable1; } finally {  } } catch (Throwable localThrowable2) { localThrowable7 = localThrowable2; throw localThrowable2; } finally {  } } catch (Throwable localThrowable3) { localThrowable6 = localThrowable3; throw localThrowable3; } finally {  } } catch (Throwable localThrowable4) { localThrowable5 = localThrowable4; throw localThrowable4;
       }
       finally
       {
         if (inFile != null) if (localThrowable5 != null) try { inFile.close(); } catch (Throwable x2) { localThrowable5.addSuppressed(x2); } else inFile.close();  
       }
     } catch (IOException ioEx) { this.logger.logClass.error("I/O Error : [" + ioEx.getMessage() + "]"); }
 
     try
     {
       Path newFilePath = Paths.get(outFileName, new String[0]);
       Path oldFilePath = this.filePath;
       int outWrittenFileSize = (int)Files.size(newFilePath);
       if (outWrittenFileSize == gunzippedExpectedFileSize)
       {
         this.filePath = newFilePath;
         Files.delete(oldFilePath);
         this.logger.logClass.debug("File " + fileToCheck + " has been unzipped");
       }
       else {
         throw new Exception("Error in gunzipping the file. Expected outputfile size=" + gunzippedExpectedFileSize + " however got size=" + outWrittenFileSize);
       }
     }
     catch (IOException ioEx) {
       this.logger.logClass.error("I/O Error : [" + ioEx.getMessage() + "]");
     }
     catch (Exception genEx) {
       this.logger.logClass.error("Generic Error : [" + genEx.getMessage() + "]");
     }
   }
 }
