 package com.bloom.dbsync.run;
 
 import java.text.ParseException;
 import java.text.SimpleDateFormat;
 import java.util.Calendar;
 import java.util.Date;
 
 public class BloomTask
 {
   public static final String TYPE_SYNC = "SYNC";
   public static final String TYPE_COMMAND = "COMMAND";
   private String taskName = "";
   private String type = "";
   private String task = "";
   private String executeTime = "";
   private long executeTimeStamp = 0L;
   private boolean isPeriodic = false;
   private String period = "";
 
   public String getTaskName() {
     return this.taskName;
   }
   public void setTaskName(String name) {
     this.taskName = name;
   }
   public String getType() {
     return this.type;
   }
   public void setType(String type) {
     this.type = type;
   }
   public String getTask() {
     return this.task;
   }
   public void setTask(String task) {
     this.task = task;
   }
   public String getExecuteTime() {
     return this.executeTime;
   }
   public void setExecuteTime(String executeTime) {
     this.executeTime = executeTime;
   }
   public String getPeriod() {
     return this.period;
   }
   public void setPeriod(String period) {
     this.period = period;
   }
   public long getExecuteTimeStamp() {
     return this.executeTimeStamp;
   }
   public void setExecuteTimeStamp(long executeTimeStamp) {
     this.executeTimeStamp = executeTimeStamp;
   }
   public boolean isPeriodic() {
     return this.isPeriodic;
   }
 
   public void updateExecuteTimeStamp() {
     SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     Date date = null;
     try {
       date = dateFormat.parse(this.executeTime);
     } catch (ParseException e) {
     }
     this.executeTimeStamp = date.getTime();
   }
 
   public void computeNextPeriod()
   {
     SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
     String numericRegex = "\\d*";
 
     Calendar calender = Calendar.getInstance();
     calender.setTimeInMillis(this.executeTimeStamp);
 
     String[] periodParts = this.period.split(" ");
 
     String[] firstPart = periodParts[0].split("-");
     if (firstPart[0].matches(numericRegex))
       calender.add(1, Integer.parseInt(firstPart[0]));
     if (firstPart[1].matches(numericRegex))
       calender.add(2, Integer.parseInt(firstPart[1]));
     if (firstPart[2].matches(numericRegex)) {
       calender.add(5, Integer.parseInt(firstPart[2]));
     }
 
     String[] secondPart = periodParts[1].split(":");
     if (secondPart[0].matches(numericRegex))
       calender.add(11, Integer.parseInt(secondPart[0]));
     if (secondPart[1].matches(numericRegex))
       calender.add(12, Integer.parseInt(secondPart[1]));
     if (secondPart[2].matches(numericRegex)) {
       calender.add(13, Integer.parseInt(secondPart[2]));
     }
     this.executeTime = dateFormat.format(calender.getTime());
     this.executeTimeStamp = calender.getTimeInMillis();
   }
 }
