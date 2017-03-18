 package com.bloom.dbsync.statistics;
 
 public class AggregatedStatistics
 {
   private String configurationName;
   private String metricName;
   private String rangeType;
   private String rangeDate;
   private long rangeTime;
   private double duration;
   private double total;
   private double throughput;
 
   public AggregatedStatistics()
   {
     this.configurationName = "";
     this.metricName = "";
     this.rangeType = "";
     this.rangeDate = "";
     this.rangeTime = 0L;
     this.duration = 0.0D;
     this.total = 0.0D;
     this.throughput = 0.0D;
   }
 
   public double getDuration()
   {
     return this.duration;
   }
 
   public double getTotal()
   {
     return this.total;
   }
 
   public double getThroughput()
   {
     return this.throughput;
   }
 
   public void setStats(double _duration, double _total, double _throughput)
   {
     this.duration = _duration;
     this.total = _total;
     this.throughput = _throughput;
   }
 
   public void setStats(String _configName, String _metric, String _rangeType, String _rangeDate, long _rangeTime, double _duration, double _total, double _throughput)
   {
     this.configurationName = _configName;
     this.metricName = _metric;
     this.rangeType = _rangeType;
     this.rangeDate = _rangeDate;
     this.rangeTime = _rangeTime;
     this.duration = _duration;
     this.total = _total;
     this.throughput = _throughput;
   }
 
   public void clear()
   {
     this.configurationName = "";
     this.metricName = "";
     this.rangeType = "";
     this.rangeDate = "";
     this.rangeTime = 0L;
     this.duration = 0.0D;
     this.total = 0.0D;
     this.throughput = 0.0D;
   }
 }
