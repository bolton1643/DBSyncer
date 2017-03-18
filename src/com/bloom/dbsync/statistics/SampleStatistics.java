 package com.bloom.dbsync.statistics;
 
 public class SampleStatistics
 {
   String configurationName;
   String fileName;
   String metricName;
   String sampleDate;
   long sampleTime;
   long duration;
   long total;
 
   public SampleStatistics()
   {
     this.configurationName = "";
     this.fileName = "";
     this.metricName = "";
     this.sampleDate = "";
     this.sampleTime = 0L;
     this.duration = 0L;
     this.total = 0L;
   }
 
   public void setStats(String _configName, String _fileName, String _metric, String _sampleDate, long _sampleTime, long _sampleDuration, long _sampleValue)
   {
     this.configurationName = _configName;
     this.fileName = _fileName;
     this.metricName = _metric;
     this.sampleDate = _sampleDate;
     this.sampleTime = _sampleTime;
     this.duration = _sampleDuration;
     this.total = _sampleValue;
   }
 
   public void clear()
   {
     this.configurationName = "";
     this.fileName = "";
     this.metricName = "";
     this.sampleDate = "";
     this.sampleTime = 0L;
     this.duration = 0L;
     this.total = 0L;
   }
 }
