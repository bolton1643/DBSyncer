 package com.bloom.dbsync.logreader;
 
 import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SFTPException;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.SFTPv3DirectoryEntry;
import ch.ethz.ssh2.SFTPv3FileAttributes;
import ch.ethz.ssh2.SFTPv3FileHandle;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

 import com.bloom.dbsync.fileLogger.LogWriter;

 import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import org.apache.commons.math3.util.MultidimensionalCounter.Iterator;
import org.apache.log4j.Logger;
 
 public class Host
 {
   private String hostName;
   private String hostIp;
   private String connUser;
   private Date connEstablished;
   private Connection sshConnection;
   private Session sshSession;
   private LogWriter logger;
   private String charsetEncoding;
 
   public Host(String hostName, String connUser)
   {
     this.hostName = hostName;
     this.connUser = connUser;
     this.logger = new LogWriter(Host.class);
     this.charsetEncoding = "UTF-8";
   }
 
   public Host(String hostName, String connUser, String connPwd) throws IOException
   {
     this.hostName = hostName;
     this.connUser = connUser;
     this.logger = new LogWriter(Host.class);
     this.charsetEncoding = "UTF-8";
     try
     {
       openSSHConnection("", connPwd);
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Failed to open SSH connection to " + this.hostName + ". Error: " + ioEx.getMessage());
       throw ioEx;
     }
   }
 
   public void openSSHConnection(String publicKeyFilePath, String password) throws IOException
   {
     this.sshConnection = new Connection(this.hostName);
     boolean isAuthenticated = false;
     this.sshConnection.connect();
 
     File publicKey = new File(publicKeyFilePath);
     if ((!publicKeyFilePath.isEmpty()) && (publicKey.exists()))
     {
       Path publicKeyFile = Paths.get(publicKeyFilePath, new String[0]);
       isAuthenticated = this.sshConnection.authenticateWithPublicKey(this.connUser, publicKeyFile.toFile(), null);
     }
     if ((publicKeyFilePath.isEmpty()) || (!publicKey.exists()) || (!isAuthenticated))
     {
       isAuthenticated = this.sshConnection.authenticateWithPassword(this.connUser, password);
       if (!isAuthenticated)
         throw new IOException(getClass().getName() + " : SSH2 Authentication to " + this.connUser + "@" + this.hostName + " failed.");
     }
     this.logger.logClass.info("SSH Authentication to [" + this.hostName + "] established succesfully.");
     this.connEstablished = new Date();
   }
 
   public InputStream runCommand(String command) throws IOException, InterruptedException
   {
     InputStream commandOutput = null;
     try
     {
       if (this.sshSession != null) this.sshSession.close();
       this.sshSession = this.sshConnection.openSession();
       this.sshSession.execCommand(command, this.charsetEncoding);
       commandOutput = new StreamGobbler(this.sshSession.getStdout());
       this.logger.logClass.debug("Finished running command: " + command);
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Failed to run remote SSH command: " + command + " on [" + this.hostName + "].");
       this.logger.logClass.error(ioEx.getMessage());
       if (this.sshSession != null) this.sshSession.close();
       throw ioEx;
     }
 
     return commandOutput;
   }
 
   public InputStream runCommand(String command, String userOsPassword) throws IOException, InterruptedException
   {
     InputStream commandOutput = null;
     try
     {
       if (this.sshConnection != null)
         this.sshConnection.close();
       openSSHConnection("", userOsPassword);
       this.sshSession = this.sshConnection.openSession();
 
       this.sshSession.execCommand(command, this.charsetEncoding);
       commandOutput = new StreamGobbler(this.sshSession.getStdout());
       this.logger.logClass.debug("Finished running command: [" + command + "]");
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Failed to run remote SSH command: " + command + " on [" + this.hostName + "].");
       this.logger.logClass.error(ioEx.getMessage());
       if (this.sshSession != null) this.sshSession.close();
       throw ioEx;
     }
 
     return commandOutput;
   }
 
   public void closeSession()
   {
     this.sshSession.close();
   }
 
   public boolean scpFile(String remoteFile, String localDirectory) throws IOException {
     long startTime = System.currentTimeMillis();
     Path remoteFilePath = Paths.get(remoteFile, new String[0]);
     Path localDirectoryPath = Paths.get(localDirectory, new String[0]);
     String remoteFileName = remoteFilePath.getFileName().toString();
     Path localFilePath = localDirectoryPath.resolve(remoteFileName);
 
     if (!Files.exists(localDirectoryPath, new LinkOption[0]))
     {
       String errorMessage = localDirectoryPath.toString() + " directory does not exists";
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
     SCPClient scpLocal = new SCPClient(this.sshConnection);
     Files.copy(scpLocal.get(remoteFile), localFilePath, new CopyOption[0]);
     if (!Files.exists(localFilePath, new LinkOption[0]))
     {
       String errorMessage = "Failed to scp, no local copy of [" + localFilePath.toString() + "]";
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
 
     long copyTime = System.currentTimeMillis() - startTime;
     long fsize = Files.size(localFilePath);
 
     this.logger.logClass.info("Copied file:" + remoteFileName + ";copy time[sec]:" + copyTime / 1000L + ";size[KB]:" + fsize / 1024L + ";copy rate[kb/sec]:" + (int)(fsize / 1024L) * 1000 / copyTime);
 
     return true;
   }
 
   public boolean scpFile(String remoteFile, String localDirectory, long rfsize) throws IOException
   {
     long startTime = System.currentTimeMillis();
     Path remoteFilePath = Paths.get(remoteFile, new String[0]);
     Path localDirectoryPath = Paths.get(localDirectory, new String[0]);
     String remoteFileName = remoteFilePath.getFileName().toString();
     Path localFilePath = localDirectoryPath.resolve(remoteFileName);
 
     if (!Files.exists(localDirectoryPath, new LinkOption[0]))
     {
       String errorMessage = localDirectoryPath.toString() + " directory does not exists";
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
     SCPClient scpLocal = new SCPClient(this.sshConnection);
     Files.copy(scpLocal.get(remoteFile), localFilePath, new CopyOption[0]);
     long fsize = Files.size(localFilePath);
     if (!Files.exists(localFilePath, new LinkOption[0]))
     {
       String errorMessage = "Failed to scp, no local copy of [" + localFilePath.toString() + "]";
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
     if (rfsize != fsize)
     {
       String errorMessage = "Failed to scp, local copy of [" + localFilePath.toString() + "] has different size than the source";
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
 
     long copyTime = System.currentTimeMillis() - startTime;
     this.logger.logClass.info("Copied file:" + remoteFileName + ";copy time[sec]:" + copyTime / 1000L + ";size[KB]:" + fsize / 1024L + ";copy rate[kb/sec]:" + (int)(fsize / 1024L) * 1000 / copyTime);
 
     return true;
   }
 
   public ArrayList getFileList(String remoteDirectory)
     throws IOException
   {
     SFTPv3Client sftp = new SFTPv3Client(this.sshConnection);
     ArrayList temp = new ArrayList(sftp.ls(remoteDirectory));
     ArrayList arr = new ArrayList();
     
     for (int i = 0 ; i < temp.size(); i++) {
    	   SFTPv3DirectoryEntry object = (SFTPv3DirectoryEntry)temp.get(i);
       arr.add(object.filename);
     }
     sftp.close();
     Collections.sort(arr);
     return arr;
   }
 
   public ArrayList getFileList(String remoteDirectory, String fromDate) throws IOException { SFTPv3Client sftp = new SFTPv3Client(this.sshConnection);
     String errorMessage = "";
     ArrayList lsVector;
     try {
       lsVector = new ArrayList(sftp.ls(remoteDirectory));
     }
     catch (SFTPException sftpEx)
     {
       sftp.close();
       if ("".equals(remoteDirectory))
       {
         errorMessage = "SFTP Error: source_archiveDirectory is not initialized correctly, check the connection to the source DB or define it manually in a configuration file";
       }
       else
       {
         errorMessage = "SFTP Error: [" + sftpEx.getMessage() + "]";
       }
       this.logger.logClass.error(errorMessage);
       throw new IOException(errorMessage);
     }
     ArrayList arr = new ArrayList();
 
     Date startDate = new Date();
     try
     {
       SimpleDateFormat sdFormat = new SimpleDateFormat("MM/dd/yyyyHH:mm:ss");
       sdFormat.setLenient(false);
       startDate = sdFormat.parse(fromDate);
     }
     catch (ParseException pe)
     {
       this.logger.logClass.error("Date format is incorrect, expected MM/dd/yyyyHH:mm:ss, got: " + fromDate);
     }
     long startUnixTime = startDate.getTime() / 1000L;
     for (int i = 0 ; i < lsVector.size(); i++){
    	   SFTPv3DirectoryEntry dirObject = (SFTPv3DirectoryEntry)lsVector.get(i);
       if (!dirObject.attributes.isDirectory()) {
         SFTPv3FileAttributes fileAttr = dirObject.attributes;
         if (fileAttr.mtime.intValue() >= startUnixTime)
         {
           arr.add(dirObject);
         }
       }
     }
     sftp.close();
     Collections.sort(arr, new Comparator()
     {
       public int compare(Object o1, Object o2) {
         SFTPv3DirectoryEntry ob1 = (SFTPv3DirectoryEntry)o1;
         SFTPv3DirectoryEntry ob2 = (SFTPv3DirectoryEntry)o2;
         return ob1.attributes.mtime.compareTo(ob2.attributes.mtime);
       }
     });
     return arr;
   }
 
   public void printHostConfig()
   {
     System.out.println("Host name                        :" + this.hostName);
     System.out.println("Connection user                  :" + this.connUser);
     System.out.println("Last type connection established :" + this.connEstablished);
   }
 
   public void close()
   {
     this.sshConnection.close();
   }
 
   public void sftpPutStringToFile(String inputString, String remoteFileName) throws IOException
   {
     SFTPv3Client sftp;
     try
     {
       sftp = new SFTPv3Client(this.sshConnection);
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Sftp Error: failed to open sftp connection to a remote host. Error:" + ioEx.getMessage());
       throw ioEx;
     }
     SFTPv3FileHandle remoteSftpFileHandle;
     try
     {
       remoteSftpFileHandle = sftp.createFileTruncate(remoteFileName);
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Sftp Error: failed to open file for writing on remote host. Error:" + ioEx.getMessage());
       throw ioEx;
     }
     byte[] inputStringByteArray = inputString.getBytes();
     try
     {
       sftp.write(remoteSftpFileHandle, 0L, inputStringByteArray, 0, inputStringByteArray.length);
     }
     catch (IOException ioEx)
     {
       this.logger.logClass.error("Sftp Error: failed to write data to a remote file. Error:" + ioEx.getMessage());
       throw ioEx;
     }
 
     this.logger.logClass.debug("Finished to write data to a remote file: " + remoteFileName);
     if (sftp != null)
     {
       if (remoteSftpFileHandle != null) sftp.closeFile(remoteSftpFileHandle);
       sftp.close();
     }
   }
 
   public boolean isOpenSSHConnection()
   {
     if (this.sshConnection.isAuthenticationComplete()) {
       return true;
     }
     return false;
   }
 
   public Connection getSshConenction()
   {
     return this.sshConnection;
   }
 
   public Session getSshSession()
   {
     return this.sshSession;
   }
 
   public String getCharsetEncoding() {
     return this.charsetEncoding;
   }
 
   public void setCharsetEncoding(String newCharsetString) throws IllegalArgumentException {
     if ("".equals(newCharsetString))
       newCharsetString = "UTF-8";
     if (Charset.isSupported(newCharsetString))
     {
       this.charsetEncoding = newCharsetString;
     }
     else
     {
       this.logger.logClass.error("Host character set is not supported" + newCharsetString);
       throw new IllegalArgumentException();
     }
   }
 }