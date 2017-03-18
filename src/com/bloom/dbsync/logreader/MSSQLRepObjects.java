 package com.bloom.dbsync.logreader;
 
 import com.bloom.dbsync.db.MSSQLDataStore;
 import com.mongodb.AggregationOutput;
 import com.mongodb.BasicDBObject;
 import com.mongodb.DB;
 import com.mongodb.DBCollection;
 import com.mongodb.DBObject;
 import com.mongodb.MongoException;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.util.ArrayList;
 import java.util.List;
 import java.util.Scanner;
 
 public class MSSQLRepObjects
 {
   static final String cmdbMSSQLDictCollectionName = "MSSQLDataDictionary";
   static final String cmdbTableKeysCollectionName = "tableKeys";
   private static DBCollection cmdbMSSQLDict;
   private static DBCollection cmdbTableKeys;
   private static DB cmdbDb;
   private String configurationName;
   private MSSQLDataStore msDs;
   ArrayList<dbObject> databaseObjects;
   String includeString = "";
   String excludeString = "";
 
   public MSSQLRepObjects(String configName, MSSQLDataStore mssqlDs) throws Exception
   {
     this.configurationName = configName;
     this.msDs = mssqlDs;
 
     this.databaseObjects = new ArrayList();
 
     this.includeString = "ALL";
     this.excludeString = "MSSQL:master;tempdb;model;msdb";
 
     createIncludeDatabases();
     createIncludeObjects();
   }
 
 
   private void createIncludeDatabases()
     throws Exception
   {
     String dbName = "";
     int dbId = -1;
 
     String inList = "";
     String ninList = "";
 
     if (!"ALL".equals(this.includeString))
     {
       Scanner s = new Scanner(this.includeString);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         String currentString = s.next();
         int finishDbName = currentString.indexOf(":");
 
         if (finishDbName == -1)
         {
           dbName = currentString;
         }
         else
         {
           dbName = currentString.substring(0, finishDbName);
         }
         inList = inList + "'" + dbName + "',";
       }
       s.close();
     }
 
     if (!inList.isEmpty())
     {
       inList = " name in (" + inList.substring(0, inList.length() - 1) + ")";
     }
 
     if (!"NONE".equals(this.excludeString))
     {
       Scanner s = new Scanner(this.excludeString);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         String currentString = s.next();
         int finishDbName = currentString.indexOf(":");
 
         if (finishDbName == -1)
         {
           dbName = currentString;
           ninList = ninList + "'" + dbName + "',";
         }
       }
       s.close();
     }
 
     if (!ninList.isEmpty())
     {
       ninList = " name not in (" + ninList.substring(0, ninList.length() - 1) + ")";
     }
 
     String query = "SELECT DB_ID(name) as dbId, name FROM sys.databases ";
     if (!inList.isEmpty())
     {
       query = query + " WHERE " + inList;
     }
     if (!ninList.isEmpty())
     {
       if (inList.isEmpty())
       {
         query = query + " WHERE " + ninList;
       }
       else
       {
         query = query + " and " + ninList;
       }
     }
 
     Statement stmt = this.msDs.getMSSQLConnection().createStatement();
     this.msDs.getMSSQLConnection().setCatalog("master");
     ResultSet rset = stmt.executeQuery(query);
     while (rset.next())
     {
       dbId = rset.getInt(1);
       dbName = rset.getString(2);
 
       this.databaseObjects.add(new dbObject(dbId, dbName, "", ""));
     }
     rset.close();
     stmt.close();
   }
 
   private void createIncludeObjects()
   {
     dbObject currentDb = null;
     String tableName = "";
     String dbName = "";
 
     List ninList = new ArrayList();
     List inList = new ArrayList();
 
     for (int i = 0; i < this.databaseObjects.size(); i++)
     {
       currentDb = (dbObject)this.databaseObjects.get(i);
 
       if (!"ALL".equals(this.includeString))
       {
         Scanner s = new Scanner(this.includeString);
         s.useDelimiter(";");
         while (s.hasNext())
         {
           boolean haveTable = false;
           String currentString = s.next();
           int finishDbName = currentString.indexOf(":");
 
           if (finishDbName == -1)
           {
             dbName = currentString;
           }
           else
           {
             dbName = currentString.substring(0, finishDbName);
             haveTable = true;
           }
 
           if (currentDb.dbName.equals(dbName))
           {
             if (!haveTable)
               break;
             String tables = currentString.replace(dbName + ":", "");
             Scanner s2 = new Scanner(tables);
             s2.useDelimiter(",");
             inList.clear();
             while (s2.hasNext())
             {
               tableName = s2.next().toString();
               inList.add(tableName);
             }
             s2.close();
             break;
           }
         }
 
         s.close();
       }
 
       if (!"NONE".equals(this.excludeString))
       {
         Scanner s = new Scanner(this.excludeString);
         s.useDelimiter(";");
         while (s.hasNext())
         {
           String currentString = s.next();
           int finishDbName = currentString.indexOf(":");
 
           if (finishDbName == -1)
           {
             dbName = currentString;
           }
           else
           {
             dbName = currentString.substring(0, finishDbName);
           }
 
           if (currentDb.dbName.equals(dbName))
           {
             String tables = currentString.replace(dbName + ":", "");
             Scanner s2 = new Scanner(tables);
             s2.useDelimiter(",");
             ninList.clear();
             while (s2.hasNext())
             {
               tableName = s2.next().toString();
               ninList.add(tableName);
             }
             s2.close();
             break;
           }
         }
         s.close();
       }
 
       if (!inList.isEmpty())
       {
         currentDb.includeObjects = getObjectsList(currentDb.dbName, inList);
         this.databaseObjects.set(i, currentDb);
       }
 
       if (!ninList.isEmpty())
       {
         currentDb.excludeObjects = getObjectsList(currentDb.dbName, ninList);
         this.databaseObjects.set(i, currentDb);
       }
     }
   }
 
   private String getObjectsList(String dbName, List objectNames)
   {
     String tablesList = "";
     String tableName = "";
     String objectId = "";
     BasicDBObject query = new BasicDBObject("configurationName", this.configurationName);
     query.append("databaseName", dbName);
     query.append("tableName", new BasicDBObject("$in", objectNames));
     DBObject groupFields = new BasicDBObject();
     groupFields.put("schemaName", "$schemaName");
     groupFields.put("tableName", "$tableName");
     groupFields.put("objectId", "$objectId");
     DBObject fields = new BasicDBObject();
     fields.put("_id", groupFields);
 
     AggregationOutput rset = null;
     try
     {
       List pipeline = new ArrayList();
       DBObject match = new BasicDBObject("$match", query);
       DBObject group = new BasicDBObject("$group", fields);
       pipeline.add(match);
       pipeline.add(group);
       rset = cmdbMSSQLDict.aggregate(pipeline);
       for (DBObject currentRecord : rset.results())
       {
         DBObject groupId = (DBObject)currentRecord.get("_id");
         objectId = groupId.get("objectId").toString();
 
         tablesList = tablesList + objectId + ",";
       }
     }
     catch (MongoException mongoEx)
     {
       throw mongoEx;
     }
     if (!tablesList.isEmpty())
     {
       tablesList = tablesList.substring(0, tablesList.length() - 1);
       tablesList = " (" + tablesList + ")";
     }
     return tablesList;
   }
 
   public String getIncludeObjects(String inDbName)
   {
     String answer = "";
     for (dbObject currentDb : this.databaseObjects)
     {
       if (currentDb.dbName.equals(inDbName))
       {
         answer = currentDb.includeObjects;
         break;
       }
     }
     return answer;
   }
 
   public String getExcludeObjects(String inDbName)
   {
     String answer = "";
     for (dbObject currentDb : this.databaseObjects)
     {
       if (currentDb.dbName.equals(inDbName))
       {
         answer = currentDb.excludeObjects;
         break;
       }
     }
     return answer;
   }
 
   public int getSize()
   {
     return this.databaseObjects.size();
   }
 
   public String getWhereDatabaseList()
   {
     String answer = " (";
     for (dbObject currentDb : this.databaseObjects)
     {
       answer = answer + "'" + currentDb.dbName + "',";
     }
     answer = answer.substring(0, answer.length() - 1) + ")";
     return answer;
   }
 
   public String getDbNameByIndex(int index)
   {
     return ((dbObject)this.databaseObjects.get(index)).dbName;
   }
 
   public int getDbIdByIndex(int index)
   {
     return ((dbObject)this.databaseObjects.get(index)).dbId;
   }
 
   public void reload() throws Exception
   {
     this.databaseObjects.clear();
     createIncludeDatabases();
     createIncludeObjects();
   }
 
   public static String getGenericInclude()
   {
     return " (select object_id from sys.tables where is_ms_shipped=0 and type = 'U') ";
   }
 
   private static class dbObject
   {
     int dbId = -1;
     String dbName = "";
     String includeObjects = "";
     String excludeObjects = "";
 
     private dbObject(int inDbId, String inDbName, String inDbObjects, String exDbObjects)
     {
       this.dbId = inDbId;
       this.dbName = inDbName;
       this.includeObjects = inDbObjects;
       this.excludeObjects = exDbObjects;
     }
   }
 }
