 package com.bloom.dbsync.logreader;
 
 public class ParserExceptions extends Exception
 {
   private int exceptionCode;
   private String exceptionMessage;
 
   public ParserExceptions(int exCode)
   {
     this.exceptionCode = exCode;
     switch (this.exceptionCode) { case -1:
       this.exceptionMessage = "Error: Generic udefinied error"; break;
     case 10:
       this.exceptionMessage = "Error: IOError - file does not exist"; break;
     default:
       this.exceptionMessage = ("Error" + exCode);
     }
   }
 
   public int getExceptionCode()
   {
     return this.exceptionCode;
   }
 
   public String getExceptionMessage() {
     return this.exceptionMessage;
   }
 
   public String toString() {
     return "BloomDBSync-" + this.exceptionCode + " :" + this.exceptionMessage;
   }
 }
