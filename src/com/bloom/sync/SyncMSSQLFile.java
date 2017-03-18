package com.bloom.sync;
 
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


import com.bloom.dbsync.db.GreenPlumDataStore;
import com.bloom.dbsync.db.MSSQLDataStore;
import com.bloom.dbsync.fileLogger.LogWriter;
import com.bloom.dbsync.logreader.MSSQLRepObjects;


 public class SyncMSSQLFile extends SyncData
 {
   private String dateFormat;
   private String dateFormatNoTZ;
   private ArrayList<syncObjects> objectsToSync;
   private String whereIncludeSchemas;
   private String whereExcludeSchemas;
   private String sourceHost;
   private String sourcePort;
   private String sourceUser;
   private String sourcePass;
   private String sourceServiceName;
   private String targetHost;
   
   private MSSQLDataStore sourceDB;
   private String supportedDataTypes;
   private int batchSize;
   private int fullSyncloggingBatchSize;	// 完全同步日志批量大小
   private int tableRecordsCount = 0;
   private long tableSyncStartTime;
   private long tableSyncEndTime;
   private long batchT1;
   private long batchT2 = 0L;
   private String configurationName;
   private MSSQLRepObjects databasesAndObjects;
   private String tzOffset;
   private Boolean convertDataToString;
   private int parallelism;
   private SimpleDateFormat outputDateFormat;
   String fileName = null;
   BufferedWriter bw = null;
   FileWriter fw = null;
   
   
   public SyncMSSQLFile(String configName)
     throws Exception
   {
     super(configName);
     this.configurationName = configName;
     this.logger = new LogWriter(SyncMSSQLFile.class);
     Properties pro = new Properties();
     pro.put("sourceHost", "192.168.0.105");
     pro.put("sourcePort", "1433");
     pro.put("sourceUser", "sa");
     pro.put("sourcePassword", "Admin123");
     pro.put("sourceServiceName", "db");
     
     pro.put("targetFileName", "test");
     
     pro.put("tzOffset","+00:00");
     pro.put("fullSyncBatchSize", "1");
     pro.put("fullSyncLoggingBatch", "10000");
     pro.put("fullSyncLoggingBatchSize", "10000");
     
     /*
      * 日期格式结构
      * 					'yyyy/MM/dd HH:mm:ss',
        					'yyyy/MM/dd HH:mm',
        					'yyyy/MM/dd',
                        'ddd MMM dd yyyy HH:mm:ss',
                        'yyyy-MM-ddTHH:mm:ss.fffffffzzz',
                        'yyyy-MM-ddTHH:mm:ss.fffzzz',
                        'yyyy-MM-ddTHH:mm:sszzz',
                        'yyyy-MM-ddTHH:mm:ss.fffffff',
                        'yyyy-MM-ddTHH:mm:ss.fff',
                        'yyyy-MM-ddTHH:mmzzz',
                        'yyyy-MM-ddTHH:mmzz',
                        'yyyy-MM-ddTHH:mm:ss',
                        'yyyy-MM-ddTHH:mm',
                        'yyyy-MM-dd HH:mm:ss',
                        'yyyy-MM-dd HH:mm',
                        'yyyy-MM-dd',
                        'HH:mm:ss',
                        'HH:mm'
      */
     
     pro.put("dateTimeFormat", "yyyy/MM/dd HH:mm:ss");
     pro.put("parallelism", 4);
     
     this.sourceHost = pro.get("sourceHost").toString();
     this.sourcePort = pro.get("sourcePort").toString();    
     this.sourceUser = pro.get("sourceUser").toString();
     this.sourcePass = pro.get("sourcePassword").toString();    
     this.sourceServiceName = pro.get("sourceServiceName").toString();
     
     this.objectsToSync = new ArrayList();
 
     this.tzOffset = pro.get("tzOffset").toString();
     if (this.tzOffset.equals(""))
     {
       this.tzOffset = pro.get("tzOffset").toString();
 
       if (this.tzOffset.equals("")) {
         this.tzOffset = "+00:00";
         this.logger.logClass.warn("Can not get source tzOffset, it is set to +00:00");
       }
 
     }
 
     this.batchSize = Integer.parseInt(pro.get("fullSyncBatchSize").toString());
     this.fullSyncloggingBatchSize = Integer.parseInt(pro.get("fullSyncBatchSize").toString()) * 10;

     this.dateFormatNoTZ = pro.get("dateTimeFormat").toString();
     this.dateFormat = (this.dateFormatNoTZ + "X");
 
 
     this.parallelism = Integer.parseInt(pro.get("parallelism").toString());
     this.outputDateFormat = new SimpleDateFormat(this.dateFormat);

 
     try
     {
       connectToSource();
     }
     catch (SQLException sqlEx)
     {
       this.logger.logClass.error("连接到源数据库失败, 错误为 [" + sqlEx.getMessage() + "]");
       throw sqlEx;
     }
 		// 连接 MSSQL 源数据库
     try
     {
       this.databasesAndObjects = new MSSQLRepObjects(this.configurationName, this.sourceDB);
     }
     catch (Exception ex)
     {
       this.logger.logClass.error("不能从数据源创建词典, 错误信息为:" + ex.getMessage());
       throw ex;
     }
     try
     {
    	   //获得需要同步的数据库和表的列表
       getObjectsList();

     }
     catch (SQLException sqlEx)
     {
       this.logger.logClass.error("处理对象列表发生错误 ");
       throw sqlEx;
     }
   }
 
   public void connectToSource() throws Exception
   {
     String url = "jdbc:sqlserver://" + this.sourceHost + ":" + this.sourcePort + ";databaseName=" + this.sourceServiceName + ";user=" + this.sourceUser + ";password=" + this.sourcePass;
 
     this.logger.logClass.debug("正在连接到源数据库");
     this.logger.logClass.debug("连接URL为:" + url);
     this.sourceDB = new MSSQLDataStore("syncMSSQLGreenPlum", url);
   }
 
   public void connectToTarget() throws Exception{ }
 	
   // 获得源数据库的表清单
   public void getObjectsList()
     throws Exception
   {
     for (int i = 0; i < this.databasesAndObjects.getSize(); i++)
     {
       String tablesQuery = "SELECT SCHEMA_NAME(Schema_id), t.name FROM sys.tables  t LEFT JOIN sys.extended_properties ep ON ep.major_id = t.object_id WHERE (ep.class_desc IS NULL OR (ep.class_desc <> 'OBJECT_OR_COLUMN' AND ep.name <> 'microsoft_database_tools_support')) AND  t.object_id in " + MSSQLRepObjects.getGenericInclude();
 
       Statement tablesStmt = this.sourceDB.getMSSQLConnection().createStatement();
       String tableName = "";
       String dbName = "";
       // 从DB列表中取得每个dbName
       ResultSet tablesRset = null;
       dbName = this.databasesAndObjects.getDbNameByIndex(i);
       String whereClause = "";
       String excludeObjects = this.databasesAndObjects.getExcludeObjects(dbName);
       if (!excludeObjects.isEmpty())
       {
         whereClause = whereClause + " and object_id not in " + excludeObjects;
       }
 
       // 创建需要同步的数据库列表
       syncObjects currentDbObjects = new syncObjects(dbName);
       try
       {
         // 设置 Catalog 数据库
    	   	 this.sourceDB.getMSSQLConnection().setCatalog(dbName);
    	   	 // 查询 SchemaName.TableName, 并将当前表添加到同步列表中
         tablesRset = tablesStmt.executeQuery(tablesQuery);
         while (tablesRset.next())
         {
           String currentTable = tablesRset.getString(1) + "." + tablesRset.getString(2);

           currentDbObjects.addObject(currentTable);
         }
         if ((tablesStmt != null) && (!tablesStmt.isClosed())) tablesStmt.close();
         if ((tablesRset != null) && (!tablesRset.isClosed())) tablesRset.close();
       }
       catch (SQLException sqlEx)
       {
         if ((tablesStmt != null) && (!tablesStmt.isClosed())) tablesStmt.close();
         if ((tablesRset != null) && (!tablesRset.isClosed())) tablesRset.close();
         throw new SQLException("Unable to get tables list from source DB .");
       }
       // 将同步的数据库列表添加到大列表中
       this.objectsToSync.add(currentDbObjects);
       if ((tablesStmt != null) && (!tablesStmt.isClosed())) tablesStmt.close();
       if ((tablesRset != null) && (!tablesRset.isClosed())) tablesRset.close(); 
     }
   }
 
 
   public String getFieldStringValue(String columnType, String columnName, ResultSet columnData)
     throws Exception
   {
     String columnValue = "";
     switch (columnType)
     {
     case "nvarchar":
     case "nchar":
     case "ntext":
       columnValue = columnData.getNString(columnName);
       break;
     case "text":
     case "sysname":
     case "varchar":
     case "char":
       columnValue = columnData.getString(columnName);
       break;
     case "tinyint":
       columnValue = String.valueOf(columnData.getByte(columnName));
       break;
     case "smallint":
       columnValue = String.valueOf(columnData.getShort(columnName));
       break;
     case "int":
       columnValue = String.valueOf(columnData.getInt(columnName));
       break;
     case "bigint":
       columnValue = String.valueOf(columnData.getLong(columnName));
       break;
     case "float":
     case "real":
       columnValue = String.valueOf(columnData.getFloat(columnName));
       break;
     case "money":
     case "smallmoney":
     case "decimal":
     case "numeric":
       columnValue = String.valueOf(columnData.getBigDecimal(columnName));
       break;
     case "datetime":
       Date datetimeVal = columnData.getTimestamp(columnName);
       SimpleDateFormat dtfNoTZ = new SimpleDateFormat(this.dateFormatNoTZ, Locale.ENGLISH);
       try
       {
         columnValue = dtfNoTZ.format(datetimeVal) + this.tzOffset;
       }
       catch (Exception ex)
       {
         this.logger.logClass.error("在一个列中格式化日期发生错误: " + columnName);
         this.logger.logClass.error("列值: " + datetimeVal + " 尝试格式化日期到:" + this.dateFormatNoTZ);
         this.logger.logClass.error("错误信息为: " + ex.getMessage());
         throw ex;
       }
 
     case "date":
       Date dateVal = columnData.getDate(columnName);
       SimpleDateFormat dfNoTZ = new SimpleDateFormat(this.dateFormatNoTZ);
       columnValue = dfNoTZ.format(dateVal) + this.tzOffset;
       break;
     case "uniqueidentifier":
       columnValue = columnData.getString(columnName);
     }
 
     return columnValue;
   }
 
   public void copyData()
     throws Exception
   {
     Statement stmt = this.sourceDB.getMSSQLConnection().createStatement();
     //对 MSSQL 数据库同步时间进行评估
     mssqlFullSyncEstimate();
     for (syncObjects currentObject : this.objectsToSync)
     {
	       int databaseId = -1;
	       String lsn = "";
	       String startLsn = "-1";
	       String dbName = currentObject.dbName;
	       this.sourceDB.getMSSQLConnection().setCatalog(dbName);
			// 查询最大LSN号
	       String lsnQuery = "select db_id('" + dbName + "'), max([Current Lsn]) from fn_dblog(null,null) where Operation = 'LOP_COMMIT_XACT'";
	       ResultSet lsnRset = stmt.executeQuery(lsnQuery);
	       if (lsnRset.next())
	       {
	         databaseId = lsnRset.getInt(1);
	         lsn = lsnRset.getString(2);
	         startLsn = MSSQLDataStore.convertLsnToDecString(lsn);
	       }
	       lsnRset.close();
	 		// 得到 Schema, Object 名称
	       for (String objectFullName : currentObject.objectsList)
	       {
	         this.tableRecordsCount = 0;
	         this.tableSyncStartTime = System.currentTimeMillis();
	         this.batchT1 = System.currentTimeMillis();
	         String schemaName = objectFullName.substring(0, objectFullName.indexOf("."));
	         String objectName = objectFullName.substring(objectFullName.indexOf(".") + 1, objectFullName.length());
	         try
	         {
				
	           String prepQuery = "SELECT scols.NAME as columnName, types.name AS colType , " + "ISNULL(scols.length, cols.max_length) AS colLen, " + "ISNULL(scols.xprec, cols.precision) AS precision, " + "ISNULL(scols.xscale, cols.scale) AS scale, " +" scols.isnullable "+ " FROM  " + dbName + ".sys.tables t " + "INNER JOIN " + dbName + ".sys.schemas s ON s.schema_id=t.schema_id " + "INNER JOIN " + dbName + ".sys.partitions partitions ON t.object_id=partitions.object_id " + "INNER JOIN " + dbName + ".sys.system_internals_partition_columns cols ON cols.partition_id = partitions.partition_id " + "LEFT OUTER JOIN " + dbName + ".sys.syscolumns scols ON scols.id = t.object_id AND scols.colid = cols.partition_column_id " + "INNER JOIN  " + dbName + ".sys.systypes types ON scols.xusertype = types.xusertype " + "WHERE is_ms_shipped = 0 " + "AND cols.is_dropped=0 " + "AND t.type = 'U' " + "AND schema_name(t.schema_id) = ? " + "AND t.name = ? " + "ORDER BY scols.colid";
	 			// 通过schemaName, tableName 来查询表中的字段信息
	           PreparedStatement objectColumns = this.sourceDB.getMSSQLConnection().prepareStatement(prepQuery);
	           objectColumns.setString(1, schemaName);	// 模式名称
	           objectColumns.setString(2, objectName);	// 表名称
	 
	           ArrayList<columnDetails> objectColumnsList = new ArrayList();
	 
	           String objectColumnsNames = "";
	           ResultSet rsetObjectColumns = objectColumns.executeQuery();
	           String columnJavaGetType = "";	// 获得column对应的Java类型
	           String columnName = "";
	           String columnType = "";
	           String columnLength = "";
	           byte precision = 0;
	           byte scale = 0;
	           int columnNulls = 1;
	           String createTableQuery = "";
	           while (rsetObjectColumns.next())
	           {
	           columnName = rsetObjectColumns.getString(1);
	           columnType = rsetObjectColumns.getString(2).trim();
	           columnLength = rsetObjectColumns.getString(3);
	           precision = rsetObjectColumns.getByte(4);
	           scale = rsetObjectColumns.getByte(5);
	           columnNulls = rsetObjectColumns.getInt(6);
	           columnDetails columnProp = new columnDetails(columnName, columnType);
	           objectColumnsList.add(columnProp);
	           if ("".equals(objectColumnsNames))
	           {
	             objectColumnsNames = columnName;
	           }
	           else
	           {
	             objectColumnsNames = objectColumnsNames + "," + columnName;
	           }
	 
	         }
	           objectColumnsNames = objectColumnsNames + '\n';
	         rsetObjectColumns.close();
	         objectColumns.close();
	         	
				
			try {
				this.fw = new FileWriter("/users/theseusyang/downloads/"+dbName+"."+schemaName+"."+objectName+".csv");
				this.bw = new BufferedWriter(fw);
				synchronized (this.bw)
		        {
					bw.write(objectColumnsNames,0,objectColumnsNames.length());
					fw.flush();
		        }
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	         System.out.println("完成表创建:"+dbName+"."+schemaName+"."+objectName);
//	         createTableStmt.close();
           // 按照 db.schema.table 树形结构查询每张表中的数据

           // 查询表中每个字段的值

	       prepQuery = new StringBuilder().append("SELECT ").append(objectColumnsNames).append(" FROM ").append(dbName).append(".").append(schemaName).append(".\"").append(objectName).append("\"").toString();
	       Statement objectData = this.sourceDB.getMSSQLConnection().createStatement();
	       objectData.setFetchSize(10000);
	       ResultSet rsetObjectData = objectData.executeQuery(prepQuery);
	       int count = 0;
	       int cnt = 0;
	       this.batchT1 = System.currentTimeMillis();
	       // 新的字符串拼装器将数据库中的数据全量采集后生成TXT文本
	       StringBuilder sb = null;
	       
	       while (rsetObjectData.next())
	       {
	         // 获得时间差
	         int columnIndex = 0;
	 
//	         rsetObjectData.getString(columnIndex);
	         
	         sb = new StringBuilder();
	         
	         long columnst1 = System.currentTimeMillis();
	         for (columnDetails currentColumn : objectColumnsList)
	         {
	           columnIndex++;
	           String columnValue = "";
	           if (rsetObjectData.getObject(columnIndex) == null)
	           {
	             columnValue = null;
	           }
	           else
	           {
	             columnValue = getFieldStringValue(currentColumn.columnType, currentColumn.columnName, rsetObjectData);
	           }
	           
	           if (columnValue != null) {
	             sb.append(columnValue).append(",");
	           }
	 
	         }
	         sb.setLength(sb.length() - 1);
	         String outputString = sb.toString()+'\n';
	         long columnst2 = System.currentTimeMillis();
        	 	
	         System.out.println(cnt++);
	        
	         count++; 
	         if (count % this.batchSize == 0)
	         {
	        	 count = 0;
	        	 synchronized (this.bw)
		     {
	        		 this.bw.write(outputString);
//			     this.bw.write(10);
			     this.bw.flush();
			 }
	        	 

	         }
	 
	         
	         
	       } 
	       
	       this.batchT2 = System.currentTimeMillis();
	       System.out.println("批量写入时间"+(batchT2-batchT1)/1000+"秒");
	       
           rsetObjectData.close();
           objectData.close();
         
           
           
//           doneSignal.await();
         if ("Failed".equals(getFullSyncStatus()))
         {
           this.logger.logClass.error("Failed to finish sync table " + dbName+"."+schemaName+ "." + objectName);
           throw new Exception("Full sync error");
         }
 
         this.tableSyncEndTime = System.currentTimeMillis();
         this.logger.logClass.info("Finished sync table " + dbName+"."+schemaName+ "." + objectName + " [" + this.tableRecordsCount + " records] in " + (int)(this.tableSyncEndTime - this.tableSyncStartTime) + " [ms]");
 
         }
         catch (Exception ex)
         {
           this.logger.logClass.error("Error on Full Sync of table " + dbName + "." + objectName);
           this.logger.logClass.debug(ex.getMessage());
           throw ex;
         }
       }
 
     }
     stmt.close();
   }
 
   
   
  
   /*
    * 从数据库的表中全量拉取数据
    */
  
   
   public void mssqlFullSyncEstimate()
   {
     boolean connected = false;
     int totalRows = 0;
     int syncCompleteTime = totalRows / this.plugNumber;
     this.logger.logClass.info("完全同步评估已经开始!");
     String query = "";
     String dbName = "";
     try
     {
       connectToSource();
       connected = true;
     }
     catch (Exception ex)
     {
       this.logger.logClass.error("连接到源数据库失败, 错误为 [" + ex.getMessage() + "]");
     }
     if (connected)
     {
       try
       {
         for (int i = 0; i < this.databasesAndObjects.getSize(); i++)
         {
           // 获得要同步的数据库
        	   dbName = this.databasesAndObjects.getDbNameByIndex(i);
			// 得到要同步的数据库所有表的总行数
           query = "select sum(i.rows)  from " + dbName + ".sys.tables t, " + dbName + ".sys.sysindexes i " + " where t.type = 'U' AND i.id = t.object_id AND i.indid in (0,1) AND t.is_ms_shipped=0 ";
 
 
           Statement stmt = this.sourceDB.getMSSQLConnection().createStatement();
           ResultSet rset = stmt.executeQuery(query);
           while (rset.next())
           {
             totalRows += rset.getInt(1);
           }
           rset.close();
           stmt.close();
         }
		  // 计算同步需要花费的时间
         syncCompleteTime = totalRows / this.plugNumber;
 
 
         this.logger.logClass.info("FullSync 评估已经完成, FullSync 将要同步 [" + totalRows + "] 条记录, 预计 [" + syncCompleteTime + "] 秒内完成");
       }
       catch (SQLException sqlEx)
       {
         this.logger.logClass.error("Cannot complete fullSync estimation on the source db,error [" + sqlEx.getMessage() + "]");
       }
     }
   }
 
   public void run()
   {
     boolean connected = false;
     try
     {
       this.logger.logClass.info("开始同步");
       System.out.println("开始同步操作");
       try
       {
         connectToSource();
         System.out.println("成功连接源数据库");

         connectToTarget();
         System.out.println("成功连接目标数据库");

         connected = true;
       }
       catch (SQLException sqlEx)
       {
         this.logger.logClass.error("连接到数据库失败 [" + sqlEx.getMessage() + "]");
       }
       
 
       if (connected)
       {
         copyData();
         
       }
 
       this.logger.logClass.info("数据同步成功结束!");
     }
     catch (InterruptedException inEx)
     {
       interrupt();
       this.logger.logClass.info("数据同步已经停止");
       updateFullSyncStatus("Stopped", this.configurationName);
       return;
     }
     catch (Exception gex)
     {
       this.logger.logClass.error("数据同步失败: " + gex.getMessage());
       System.out.println(gex.getMessage());
     }
   }
 
   static class columnDetails
   {
     String columnName = "";
     String columnType = "";
     String columnJavaGetType = "";
 
     public columnDetails(String inColumnName, String inColumnType)
     {
       this.columnName = inColumnName;
       this.columnType = inColumnType;
     }
 
     public columnDetails(String inColumnName, String inColumnType, String inColumnJavaGetType) {
       this.columnName = inColumnName;
       this.columnType = inColumnType;
       this.columnJavaGetType = inColumnJavaGetType;
     }
   }
 
   static class mssqlRowsRange
   {
     int startRowNumber = 0;
     int endRowNumber = 0;
 
     public mssqlRowsRange(int inStart, int inEnd)
     {
       this.startRowNumber = inStart;
       this.endRowNumber = inEnd;
     }
   }
   
   private class syncObjects
   {
     String dbName = "";
     ArrayList<String> objectsList = new ArrayList();
 
     public syncObjects(String inDbName)
     {
       this.dbName = inDbName;
     }
 
     public void addObject(String inObjectName)
     {
       this.objectsList.add(inObjectName);
     }
 
     public int getSize()
     {
       return this.objectsList.size();
     }
   }
   
   public static void main(String[] args){
	   try {
		   Thread syncThread = new Thread(new  SyncMSSQLFile("testmssql2file"));
		   syncThread.start();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   }
 }
