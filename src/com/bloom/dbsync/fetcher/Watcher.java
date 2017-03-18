 package com.bloom.dbsync.fetcher;
 
 import com.bloom.dbsync.fileLogger.LogWriter;

 import java.io.IOException;
 import java.nio.file.FileSystem;
 import java.nio.file.FileSystems;
 import java.nio.file.Path;
 import java.nio.file.StandardWatchEventKinds;
 import java.nio.file.WatchEvent;
 import java.nio.file.WatchEvent.Kind;
 import java.nio.file.WatchKey;
 import java.nio.file.WatchService;
 import java.util.ArrayList;
 import java.util.Iterator;
 import java.util.List;
 import org.apache.log4j.Logger;
 
 public class Watcher extends Thread
 {
   private WatchService watcher;
   private WatchKey registerKey;
   public List<Path> files;
   private Path directory;
   private LogWriter logger;
 
   public Watcher(Path _directory)
   {
     try
     {
       this.files = new ArrayList();
       this.logger = new LogWriter(Watcher.class);
       this.directory = _directory;
 
       this.watcher = FileSystems.getDefault().newWatchService();
       this.registerKey = this.directory.register(this.watcher, new WatchEvent.Kind[] { StandardWatchEventKinds.ENTRY_CREATE });
     }
     catch (IOException ex) {
       ex.printStackTrace();
     }
   }
 
   public void run()
   {
     this.logger.logClass.info("watcher on local archive log - start");
     WatchKey key = null;
     while (true)
       try {
         key = this.watcher.take();
 
         if (key == this.registerKey) {
           Iterator i$ = key.pollEvents().iterator(); if (i$.hasNext()) { WatchEvent event = (WatchEvent)i$.next();
             WatchEvent ev = event;
             Path name = (Path)ev.context();
             Path child = this.directory.resolve(name);
 
             this.logger.logClass.info("new file copied: " + child);
             this.files.add(child);
             continue; }
         }
         sleep(2000L);
       }
       catch (InterruptedException ex)
       {
         ex.printStackTrace();
       }
   }
 }
