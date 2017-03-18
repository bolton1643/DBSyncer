package com.bloom.dbsync.logreader;

import com.bloom.dbsync.fetcher.Watcher;
import com.bloom.dbsync.fileLogger.LogWriter;

public abstract class LogReader extends Thread
{
  protected String checkPointLocation;
  protected String archiveLogDirectory;
  protected String outputDirectory;
  protected String parsedDirectory;
  protected LogWriter logger;
  protected Watcher watcher;
}
