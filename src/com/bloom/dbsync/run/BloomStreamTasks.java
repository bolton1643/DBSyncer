 package com.bloom.dbsync.run;
 
 import java.util.ArrayList;
 
 public class BloomStreamTasks
 {
   private String name = "";
   private ArrayList<BloomTask> streamTaskList = new ArrayList();
 
   public ArrayList<BloomTask> getStreamTaskList()
   {
     return this.streamTaskList;
   }
 
   public void removeTask(BloomTask task)
   {
     if (this.streamTaskList.contains(task))
       this.streamTaskList.remove(task);
   }
 
   public void addTask(BloomTask task)
   {
     this.streamTaskList.add(task);
   }
 
   public String getName() {
     return this.name;
   }
 
   public void setName(String name) {
     this.name = name;
   }
 }
