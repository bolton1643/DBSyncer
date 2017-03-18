 package com.bloom.dbsync.run;
 
 import java.util.ArrayList;
 
 public class ExternalAPIMap
 {
   public String configurationName = "";
   public ArrayList<APIMap> APIList;
 
   public ExternalAPIMap()
   {
     this.APIList = new ArrayList();
   }
 
   public APIMap getAPIMap(String operation, String databaseName, String schemaName, String objectName)
   {
     APIMap externalAPIMap = new APIMap();
     for (APIMap currentMap : this.APIList)
     {
       if ((currentMap.operation.toUpperCase().equals(operation)) && (currentMap.originalObject.databaseName.equals(databaseName)) && (currentMap.originalObject.schemaName.equals(schemaName)) && (currentMap.originalObject.objectName.equals(objectName)))
       {
         externalAPIMap = currentMap;
         break;
       }
     }
     return externalAPIMap;
   }
 
   public class APIMap
   {
     public String module = "";
     public String type = "";
     public String operation = "";
     public String scope = "";
 
     public ExternalAPIMap.dbObject originalObject = new ExternalAPIMap.dbObject();
     public ExternalAPIMap.APIDetails externalAPI = new ExternalAPIMap.APIDetails();
 
     public APIMap() {
     }
     public boolean isEmpty() { if (this.type.isEmpty())
       {
         return true;
       }
 
       return false;
     }
   }
 
   public class APIDetails
   {
     public String type = "";
     public String async = "";
     public String name = "";
 
     public APIDetails()
     {
     }
   }
 
   public class dbObject
   {
     public String databaseName = "";
     public String schemaName = "";
     public String objectName = "";
 
     public dbObject()
     {
     }
   }
 }