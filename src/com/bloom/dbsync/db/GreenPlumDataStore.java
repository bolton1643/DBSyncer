 package com.bloom.dbsync.db;
 
 import com.bloom.dbsync.fileLogger.LogWriter;
import com.google.api.client.util.Types;

 import java.io.PrintStream;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Scanner;

 import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.ds.PGPoolingDataSource;
import org.postgresql.ds.PGSimpleDataSource;
 
 public class GreenPlumDataStore extends DataStore
 {
   private PGPoolingDataSource gpDataSource;
   private Connection gpConnection;
   private Properties connInfo = new Properties();
 
   public GreenPlumDataStore(String dsLogicalName, String dsConnectionUrl) throws SQLException {
     super(dsLogicalName, dsConnectionUrl, "GreenplumSQL");
     this.logger = new LogWriter(GreenPlumDataStore.class);
     this.gpDataSource = new PGPoolingDataSource();
     this.gpDataSource.setUrl(dsConnectionUrl);
 
     this.gpConnection = this.gpDataSource.getConnection();
     this.logger.logClass.info("连接到 " + dsConnectionUrl);
 
     Statement stm = this.gpConnection.createStatement();
     ResultSet rs = stm.executeQuery("select * from gp_segment_configuration;");  
     while (rs.next()) {  
         System.out.print(rs.getString(1));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(2));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(3));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(4));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(5));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(6));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(7));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(8));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(9));  
         System.out.print("    |    ");  
         System.out.print(rs.getString(10));  
         System.out.print("    |    ");  
         System.out.println(rs.getString(11));  
     }
 
     rs.close();
     rs = null;
     stm.close();
     stm = null;
   }
 
   public Connection getGPConnection() {
     return this.gpConnection;
   }
 
   public DataSource getGPDataSource() {
     return this.gpDataSource;
   }
 
   public Properties getConnectionInfo() {
     return this.connInfo;
   }
 
 
 
   public void printDataStoreInfo() {
     System.out.println("************** Data Store Info *******************************");
     System.out.println("Data Store type             : " + getDsType());
     System.out.println("Data Store logcial name     : " + getDsLogicalName());
     System.out.println("Data Store connection URL   : " + getDsConnectionUrl());
     System.out.println("************** Data Store Info *******************************");
   }
 
 
 
   public static String createStringIncludeSchemaAndTable(String schemasDefinition, boolean addNullOwner, String ownerColmnName) {
     String answer = "";
     boolean firstTime = true;
     if (!"ALL".equals(schemasDefinition))
     {
       answer = answer + " and (";
       Scanner s = new Scanner(schemasDefinition);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         boolean haveTable = false;
         if (!firstTime)
           answer = answer + " or ";
         String currentString = s.next();
         int finishSchemaName = currentString.indexOf(":");
         String schemaName;
         if (finishSchemaName == -1) {
           schemaName = currentString;
         }
         else {
           schemaName = currentString.substring(0, finishSchemaName);
           haveTable = true;
         }
 
         answer = answer + "(" + ownerColmnName + " = '" + schemaName + "'";
         if (haveTable)
         {
           String tables = currentString.replace(schemaName + ":", "");
           answer = answer + " and TABLE_NAME in (";
           Scanner s2 = new Scanner(tables);
           s2.useDelimiter(",");
           while (s2.hasNext())
           {
             answer = answer + "'" + s2.next() + "',";
           }
           answer = answer.substring(0, answer.length() - 1);
           s2.close();
           answer = answer + ")";
         }
         answer = answer + ")";
 
         firstTime = false;
       }
 
       if (addNullOwner)
       {
         answer = answer + " OR (" + ownerColmnName + " is NULL and operation='COMMIT') " + " OR (" + ownerColmnName + " is NULL and operation='DDL' and INFO like 'USER DDL%')";
       }
 
       s.close();
       answer = answer + ")";
     }
     return answer.toUpperCase();
   }
 
   public static String createStringExcludeSchemaAndTable(String schemasDefinition, String ownerColmnName) {
     String answer = "";
     if (!"NONE".equals(schemasDefinition))
     {
       String withTable = "";
       String withoutTable = "";
       Scanner s = new Scanner(schemasDefinition);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         String currentString = s.next();
         int finishSchemaName = currentString.indexOf(":");
 
         if (finishSchemaName == -1)
         {
           String schemaName = currentString;
           withoutTable = withoutTable + "'" + schemaName + "',";
         }
         else
         {
           String schemaName = currentString.substring(0, finishSchemaName);
           String tables = currentString.replaceAll(schemaName + ":", "");
           Scanner s2 = new Scanner(tables);
           s2.useDelimiter(",");
           while (s2.hasNext())
           {
             String table = s2.next();
             withTable = withTable + "'" + schemaName + "." + table + "',";
           }
           s2.close();
         }
       }
       s.close();
       if (!"".equals(withTable))
       {
         withTable = withTable.substring(0, withTable.length() - 1);
         answer = answer + " and (" + ownerColmnName + "||'.'||TABLE_NAME not in(" + withTable + "))";
       }
       if (!"".equals(withoutTable))
       {
         withoutTable = withoutTable.substring(0, withoutTable.length() - 1);
         answer = answer + " and (nvl(" + ownerColmnName + ",'NoOwner') not in(" + withoutTable + "))";
       }
     }
     return answer.toUpperCase();
   }
 
   public static String createStringIncludeCommands(String includeCommands) {
     String answer = "";
     boolean firstTime = true;
     if (!"ALL".equals(includeCommands))
     {
       answer = answer + " and OPERATION IN (";
       Scanner s = new Scanner(includeCommands);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         String currentCommand = s.next();
         if (firstTime)
         {
           answer = answer + "'" + currentCommand + "'";
         }
         else
         {
           answer = answer + ",'" + currentCommand + "'";
         }
         firstTime = false;
       }
       s.close();
       answer = answer + ") ";
     }
     return answer.toUpperCase();
   }
 
   public static String createStringExcludeCommands(String excludeCommands) {
     String answer = "";
     boolean firstTime = true;
     if (!"NONE".equals(excludeCommands))
     {
       answer = answer + " and OPERATION NOT IN (";
       Scanner s = new Scanner(excludeCommands);
       s.useDelimiter(";");
       while (s.hasNext())
       {
         String currentCommand = s.next();
         if (firstTime)
         {
           answer = answer + "'" + currentCommand + "'";
         }
         else
         {
           answer = answer + ",'" + currentCommand + "'";
         }
         firstTime = false;
       }
       s.close();
       answer = answer + ") ";
     }
     return answer.toUpperCase();
   }
 
   public void closeGPConnection() {
     try {
       if (this.gpConnection != null) this.gpConnection.close(); 
     }
     catch (SQLException sqlEx) { this.logger.logClass.error("Failed to close Greenplum DB connection " + this.dsConnectionUrl + ". Error: " + sqlEx.getMessage()); }
 
   }
 
   public String getPostgresType(String dbType, String sourceType, String sourceLength)
   {
     String gpType = "";
     if(sourceLength == "-1"||sourceLength.equals("-1"))
    	 sourceLength = "10485760";
     switch (sourceType.toUpperCase())
     {
     
	  case "BINARY":
	  case "VARBINARY":
	  case "IMAGE":
		  	  
		 gpType = "BYTEA";
        break;
	  case "BIT":
		 gpType = "INT";
			 break; 
	  case "BOOLEAN":
		 gpType = "BOOLEAN";
		 break;
	  case "CHAR":
		 gpType = "CHAR(" + sourceLength + ")";
		 break; 
	  case "NCHAR":
	  case "VARCHAR":
	  case "NVARCHAR":  
	  case "SYSNAME":
	    gpType = "VARCHAR(" + sourceLength + ")";
	    break;

	  
		  
	  case "TEXT":
	  case "NTEXT":
       gpType = "TEXT";
       break;
	  case "TINYINT":
		gpType = "SMALLINT";
		break;
	  case "SMALLINT":
		gpType = "SMALLINT";
		break;
	  case "INT":
		gpType = "INT";
		break;
	  case "INTEGER":
			gpType = "INTEGER";
			break;
	  case "BIGINT":
			gpType = "BIGINT";
			break;
	  case "DOUBLE":
			gpType = "DOUBLE PRECISION";
			break;
					
	  case "FLOAT":
	  case "REAL":
	  case "SMALLMONEY":
	  case "MONEY":
	  case "NUMERIC":
			gpType = "NUMERIC";
			break;
	  case "DECIMAL":
			gpType = "DECIMAL";
			break;
	  case "SMALLDATETIME":
		  	gpType = "VARCHAR(100)";
			break;	
	  case "DATETIME":
		  	gpType = "DATE";
			break;
	  case "DATE":
		  	gpType = "DATE";
			break;
	  case "TIME":
		  	gpType = "TIME";
			break;
	  case "TIMESTAMP":
		  	if(dbType=="MSSQL")
		  		gpType = "BYTEA";
		  	else
		  		gpType = "TIMESTAMP";
			break;
	  
	  case "UNIQUEIDENTIFIER":
		  	gpType = "TEXT";
		  	break;
	  	
			
     }
 
     return gpType;
   }
 
   public void addFieldValue(String fieldType, int fieldPosition, Object fieldValue, PreparedStatement commandStatement, SimpleDateFormat inputDateFormat) throws Exception
   {
     switch (fieldType.toUpperCase())
     {
	  case "BINARY":
	  case "VARBINARY":
	  case "IMAGE":

		  byte[] byteValue = (byte[])fieldValue;
		  if (byteValue == null)
		  {
			  commandStatement.setNull(fieldPosition, java.sql.Types.BINARY);
		  }
		  else
		  {
			  commandStatement.setBytes(fieldPosition, byteValue);
		  }
		  break;
	  case "BIT":
	  case "BOOLEAN":
		  boolean boolValue = (boolean)fieldValue;
		  	  commandStatement.setBoolean(fieldPosition, boolValue);
		  break;
	  case "CHAR":
	  case "NCHAR":
	  case "VARCHAR":
	  case "NVARCHAR":
	  case "SYSNAME":
	  case "SMALLDATETIME":
		String strValue = (String)fieldValue;
       if ((strValue == null || strValue.isEmpty()) || (strValue.equals("null")) || (strValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.VARCHAR);
       }
       else
       {
         commandStatement.setString(fieldPosition, strValue);
       }
       break;
     case "TEXT":
     case "NTEXT":
		String txtValue = (String)fieldValue;
       if ((txtValue == null ||txtValue.isEmpty()) || (txtValue.equals("null")) || (txtValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.CLOB);
       }
       else
       {
         commandStatement.setString(fieldPosition, txtValue);
       }
       break;

     case "TINYINT":
     case "SMALLINT":
		Short sintValue =  (Short)fieldValue;
       if ((sintValue == null) || (sintValue.equals("null")) || (sintValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.TINYINT);
       }
       else
       {
         commandStatement.setShort(fieldPosition, sintValue);
       }
       break;

	  case "INT":
	  case "INTEGER":
		Integer intValue =  (Integer)fieldValue;
       if ((intValue == null) || (intValue.equals("null")) || (intValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.INTEGER);
       }
       else
       {
         commandStatement.setInt(fieldPosition, intValue);
       }
       break;

	  case "BIGINT":
		Long longValue =  (Long)fieldValue;  
       if ((longValue == null) || (longValue.equals("null")) || (longValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.BIGINT);
       }
       else
       {
         commandStatement.setLong(fieldPosition, longValue);
       }
       break;
	  
	  case "REAL":
	  case "FLOAT":
		Float floatValue =  (Float)fieldValue;    
       if ((floatValue == null) || (floatValue.equals("null")) || (floatValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.FLOAT);
       }
       else
       {
         commandStatement.setFloat(fieldPosition, floatValue);
       }
       break;

	  
     case "MONEY":
 		BigDecimal moneyValue = (BigDecimal)fieldValue;

         if ((moneyValue == null) || (moneyValue.equals("null")) || (moneyValue.equals("NULL")))
         {
           commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
         }
         else
         {
           commandStatement.setBigDecimal(fieldPosition, moneyValue);
         }
         break;
     case "SMALLMONEY":
     case "DECIMAL":
		BigDecimal decimalValue = (BigDecimal)fieldValue;
       if ((decimalValue == null) || (decimalValue.equals("null")) || (decimalValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.DECIMAL);
       }
       else
       {
         commandStatement.setBigDecimal(fieldPosition, decimalValue);
       }
       break;	  

     case "NUMERIC":
		BigDecimal numbericValue = (BigDecimal)fieldValue;
       if ((numbericValue == null) || (numbericValue.equals("null")) || (numbericValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.NUMERIC);
       }
       else
       {
         commandStatement.setBigDecimal(fieldPosition, numbericValue);
       }
       break;

	  case "DOUBLE":
		Double doubleValue = (Double)fieldValue;  
       if ((doubleValue == null) || (doubleValue.equals("null")) || (doubleValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.DOUBLE);
       }
       else
       {
         commandStatement.setDouble(fieldPosition, doubleValue);
       }
       break;
     case "DATE":
		String dateValue = (String)fieldValue; 
       if ((dateValue == null) || (dateValue.equals("null")) || (dateValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.DATE);
       }
       else
       {
         commandStatement.setDate(fieldPosition, new java.sql.Date(inputDateFormat.parse(dateValue).getTime()));
       }
       break;
     case "TIME":
		String timeValue = (String)fieldValue; 

       if ((timeValue == null) || (timeValue.equals("null")) || (timeValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.TIME);
       }
       else
       {
         commandStatement.setTime(fieldPosition, new Time(inputDateFormat.parse(timeValue).getTime()));
       }
       break;
     case "DATETIME":
     case "TIME WITH TIME ZONE":
     case "TIMESTAMP WITH TIME ZONE":
     case "TIMESTAMP":
		String timestampValue = (String)fieldValue; 
       if ((timestampValue == null || timestampValue.isEmpty()) || (fieldValue.equals("null")) || (fieldValue.equals("NULL")))
       {
         commandStatement.setNull(fieldPosition, java.sql.Types.TIMESTAMP);
       }
       else
       {
         commandStatement.setTimestamp(fieldPosition, new Timestamp(inputDateFormat.parse(timestampValue).getTime()));
       }
       break;
     
     
     
     
     
     }
   }
 }
