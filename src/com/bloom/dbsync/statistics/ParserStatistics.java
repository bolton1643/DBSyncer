 package com.bloom.dbsync.statistics;
 
 public class ParserStatistics
 {
   public String startTime;
   public String EndTime;
   public String commitTime;
   public String tableName;
   public String userName;
   public long insertsCount;
   public long deleteCount;
   public long updateCount;
   public long fileSeq;
 
   public ParserStatistics(String _startTime, String _endTime, String _commitTime, String _tableName, String _userName, long _fileSeq)
     throws Exception
   {
     this.startTime = _startTime;
 
     this.commitTime = _commitTime;
     this.tableName = _tableName;
     this.userName = _userName;
     this.fileSeq = _fileSeq;
     this.insertsCount = 0L;
     this.deleteCount = 0L;
     this.updateCount = 0L;
   }
 
   public boolean equals(Object mobj)
   {
     if (this == mobj)
       return true;
     if (mobj == null)
       return false;
     if (getClass() != mobj.getClass())
       return false;
     ParserStatistics ps = (ParserStatistics)mobj;
     if ((this.tableName.equals(ps.tableName)) && (this.userName.equals(ps.userName)) && (this.commitTime.equals(ps.commitTime)))
     {
       return true;
     }
 
     return false;
   }
 
   public int hashCode()
   {
     return this.tableName.hashCode() + this.userName.hashCode() + this.commitTime.hashCode();
   }
 }
