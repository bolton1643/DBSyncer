 package com.bloom.dbsync.run;
 
 import java.util.ArrayList;
 
 public class MetaDataMap
 {
   public String configurationName = "";
   public ArrayList<objectMap> objectList;
 
   public MetaDataMap()
   {
     this.objectList = new ArrayList();
   }
 
   public objectMap getFieldMap(String databaseName, String schemaName, String objectName, String fieldName)
   {
     objectMap mappedObject = new objectMap();
     for (objectMap currentMap : this.objectList)
     {
       if ((currentMap.type.toLowerCase().equals("field")) && (currentMap.originalObject.databaseName.equals(databaseName)) && (currentMap.originalObject.schemaName.equals(schemaName)) && (currentMap.originalObject.objectName.equals(objectName)) && (currentMap.originalObject.fieldName.equals(fieldName)))
       {
         mappedObject = currentMap;
         break;
       }
     }
     return mappedObject;
   }
 
   public objectMap getObjectMap(String databaseName, String schemaName, String objectName)
   {
     objectMap mappedObject = new objectMap();
     for (objectMap currentMap : this.objectList)
     {
       if ((currentMap.type.toLowerCase().equals("object")) && (currentMap.originalObject.databaseName.equals(databaseName)) && (currentMap.originalObject.schemaName.equals(schemaName)) && (currentMap.originalObject.objectName.equals(objectName)))
       {
         mappedObject = currentMap;
         break;
       }
     }
     return mappedObject;
   }
 
   public objectMap getObjectMap(String objectName)
   {
     objectMap mappedObject = new objectMap();
     for (objectMap currentMap : this.objectList)
     {
       if ((currentMap.type.toLowerCase().equals("object")) && (currentMap.originalObject.objectName.equals(objectName)))
       {
         mappedObject = currentMap;
         break;
       }
     }
     return mappedObject;
   }
 
   public class objectMap
   {
     public String module = "";
     public String type = "";
     public String action = "";
     public MetaDataMap.dbObject originalObject = new MetaDataMap.dbObject();
     public MetaDataMap.dbObject mappedObject = new MetaDataMap.dbObject();
 
     public objectMap()
     {
     }
 
     public boolean isEmpty()
     {
       if (this.type.isEmpty())
       {
         return true;
       }
 
       return false;
     }
   }
 
   public class dbObject
   {
     public String databaseName = "";
     public String schemaName = "";
     public String objectName = "";
     public String fieldName = "";
 
     public dbObject()
     {
     }
   }
 }
