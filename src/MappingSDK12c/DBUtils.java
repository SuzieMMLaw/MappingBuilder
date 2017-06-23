package MappingSDK12c;
import java.io.*;
import java.text.*;
import java.util.*;
import java.sql.*;

public class DBUtils{

  private static  Connection dbcon = null;
  private static  Statement st = null;
  private static  ResultSet rs = null;
//  private static  ResultSetMetaData md = null;
  private static String errorMessages;

  public static Connection startDB(){
    //Connection info
    //  jdbc:oracle:<drivertype>:@<database>
    String url=Configuration.DBurl;
    String username=Configuration.DBusername;
    String password=Configuration.DBpassword;


    try{

			Class.forName("oracle.jdbc.OracleDriver");
      dbcon = DriverManager.getConnection(url, username, password);

    } catch(java.lang.ClassNotFoundException e) {
			System.out.print("ClassNotFoundException: ");
      System.out.println(e.getMessage());
    }catch(SQLException e) {
      e.printStackTrace();
    }
    return dbcon;
  }

  public static boolean checkifLocked(String owner, String table){
	  boolean islocked=false;
	  try{
		 Connection dbcon =startDB(); 
		 String sql="SELECT LPAD(' ',DECODE(l.xidusn,0,3,0)) || l.oracle_username \""+ owner+"\", "+
				 	"o.owner, o.object_name, o.object_type "+
				 	"FROM v$locked_object l, dba_objects o "+
				 	"WHERE l.object_id = o.object_id "+
				    "ORDER BY o.object_id, 1 desc ";
		 st=dbcon.createStatement();
		 rs=st.executeQuery(sql);
		 while(rs.next()){
			 if(rs.getString("owner").equals(owner) && rs.getString("object_name").equals(table)){
				 islocked=true;
				 System.out.println("Warning: Table \""+owner+"."+table+"\" is locked. The application might not run as expected...");
				 break;
			 }
		 }
	  }catch(SQLException e){
		  e.printStackTrace();
	  }finally{
		closeDB(dbcon);  
	  }
	  return islocked;	  
  }

  public static List<MappingMetaData> getMappingInfo(){
    List<MappingMetaData> mmd_list=new LinkedList<MappingMetaData>();
    try{
      Connection dbcon=startDB();
      String owner="OCDM_ODI_STAGE";
      String sql= "SELECT SOURCE_MODEL, SOURCE_TABLE, TARGET_MODEL, TARGET_TABLE, FILTER_CONDITION, MAPPING_NAME "+
                  "FROM "+owner+". MPNGS_MTDT " +
                  "WHERE STATUS='PENDING'";

      if (dbcon == null){
        errorMessages = "You must establish a connection first!";
      }
      checkifLocked(owner, "MPNGS_MTDT");
      st = dbcon.createStatement();
      rs = st.executeQuery(sql);

      String target_model_name, source_model_name, target_datastore, source_datastore, filter_condition, mapping_name;

      while(rs.next()){
        source_model_name=rs.getString("SOURCE_MODEL");
        target_model_name=rs.getString("TARGET_MODEL");
        target_datastore=rs.getString("TARGET_TABLE");
        source_datastore=rs.getString("SOURCE_TABLE");
        filter_condition=rs.getString("FILTER_CONDITION");
        mapping_name=rs.getString("MAPPING_NAME");

        MappingMetaData mmd = new MappingMetaData(mapping_name, source_model_name, target_model_name, target_datastore, source_datastore, filter_condition);
        mmd_list.add(mmd);
      }
    }catch(SQLException e) {
        System.out.println(errorMessages);
        e.printStackTrace();
    }finally{

      closeDB(dbcon);
    }

    return mmd_list;
  }



  public static void updateMappingStatus(String newStatus, String mappingName){
	  
	  String owner="OCDM_ODI_STAGE";
	  if(!checkifLocked(owner, "MPNGS_MTDT")){
		try{
	      Connection dbcon = startDB();
	      String sql= "UPDATE "+owner+".MPNGS_MTDT "+
	                  "SET STATUS='"+newStatus+"' WHERE MAPPING_NAME='"+mappingName+"'";
	      if (dbcon == null){
	        errorMessages = "You must establish a connection first!";
	      }
	      st = dbcon.createStatement();
	      st.executeUpdate(sql);
	      dbcon.setAutoCommit(false);
	      dbcon.commit();
	    }catch(SQLException e) {
	        System.out.println(errorMessages);
	        e.printStackTrace();
	    }finally{
	      closeDB(dbcon);
	    }
	  }else{
		  System.out.println("Warning: OCDM_ODI_STAGE.MPNGS_MTDT might not be up to date.");
	  }
    
    return;
  }




  public static List<ColumnMetaData> getColMetaData(String table, String owner){
    List<ColumnMetaData> cmd_list=new LinkedList<ColumnMetaData>();
    try{

      Connection dbcon = startDB();
      String sql= "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, CHAR_LENGTH "+
                  "FROM ALL_TAB_COLUMNS "+
                  "WHERE TABLE_NAME='"+table+"' AND OWNER='"+owner+"'";

      st = dbcon.createStatement();

      if (dbcon == null){
        errorMessages = "You must establish a connection first!";
      }
      rs = st.executeQuery(sql);
      String name, type;
      Integer scale;
      boolean nullable;
      Integer logical_length;
      while(rs.next()){

        name=rs.getString("COLUMN_NAME");
        type=rs.getString("DATA_TYPE");
        scale=rs.getInt("DATA_SCALE");
        if (ODIUtils.nameisLike(rs.getString("NULLABLE"), "N" )){
          nullable=false;
        }else{
          nullable=true;
        }

        if (type.equals("NUMBER")){
          logical_length=rs.getInt("DATA_PRECISION");
        }else if(type.equals("CHAR")){
          logical_length=rs.getInt("CHAR_LENGTH");
        }else if(type.equals("NCHAR")){
          logical_length=rs.getInt("CHAR_LENGTH");
        }else if(type.equals("VARCHAR2")){
          logical_length=rs.getInt("CHAR_LENGTH");
        }else if(type.equals("NVARCHAR2")){
          logical_length=rs.getInt("CHAR_LENGTH");
        }else if(ODIUtils.nameisLike(type, "TIMESTAMP.*")){
          logical_length=rs.getInt("DATA_SCALE");
        }else if(type.equals("DATE")){
          logical_length=-1;
        }else{
          logical_length=nvl(rs.getInt("DATA_PRECISION"), rs.getInt("DATA_LENGTH"));
        }


        ColumnMetaData cmd = new ColumnMetaData(table, name, type, logical_length, scale, nullable);
        cmd_list.add(cmd);
      }

    }catch(SQLException e) {
        System.out.println(errorMessages);
        e.printStackTrace();
    }finally{
      closeDB(dbcon);
    }

    return cmd_list;
  }


  public static Integer nvl(Integer p, Integer l){
    int value=p;
    if (p==null){
      value=l;
    }
    return value;
  }

  public static List<ConstraintMetaData> getConMetaData(String table, String owner){
    List<ConstraintMetaData> cmd_list=new LinkedList<ConstraintMetaData>();
    try{

      Connection dbcon = startDB();
      String sql= "SELECT AC.CONSTRAINT_TYPE c, AC.CONSTRAINT_NAME cn, AC.TABLE_NAME tn, ACC.COLUMN_NAME cln "+
                  "FROM ALL_CONSTRAINTS AC, ALL_CONS_COLUMNS ACC "+
                  "WHERE AC.TABLE_NAME='"+table+"' AND AC.OWNER='"+owner+"' AND "+
                  "AC.CONSTRAINT_NAME=ACC.CONSTRAINT_NAME AND "+
                  "AC.OWNER=ACC.OWNER AND "+
                  "AC.STATUS='ENABLED' AND "+
                  "CONSTRAINT_TYPE IN ('U', 'P')";

      st = dbcon.createStatement();
      if (dbcon == null){
        errorMessages = "You must establish a connection first!";
  		}
      rs = st.executeQuery(sql);
      String constraint_type, constraint_name, table_name, column_name;

      while (rs.next()){
        constraint_type=rs.getString("c");
        constraint_name=rs.getString("cn");
        table_name=rs.getString("tn");
        column_name=rs.getString("cln");
        ConstraintMetaData cmd = new ConstraintMetaData(constraint_type, constraint_name, table_name, column_name);

        cmd_list.add(cmd);
      }
    }catch (SQLException e){
        System.out.println(errorMessages);
        e.printStackTrace();
    }finally{
      closeDB(dbcon);
    }
    return cmd_list;
  }


  public static List<ConstraintRefMetaData> getConRefMetaData(String table, String owner){

    List<ConstraintRefMetaData> crmd_list = new LinkedList<ConstraintRefMetaData>();

    try{

      Connection dbcon = startDB();
      String sql= "SELECT AC.CONSTRAINT_NAME c, AC.TABLE_NAME tn, ACC.COLUMN_NAME cn, ACC1.TABLE_NAME rtn, ACC1.COLUMN_NAME rcn "+
            "FROM ALL_CONSTRAINTS AC, ALL_CONS_COLUMNS ACC, ALL_CONS_COLUMNS ACC1 "+
            "WHERE AC.TABLE_NAME='"+table+"' AND AC.OWNER='"+owner+"' AND "+
            "AC.CONSTRAINT_TYPE='R' AND "+
            "AC.CONSTRAINT_NAME=ACC.CONSTRAINT_NAME AND "+
            "AC.R_CONSTRAINT_NAME=ACC1.CONSTRAINT_NAME AND "+
            "AC.OWNER=ACC.OWNER AND AC.R_OWNER=ACC1.OWNER AND "+
            "AC.STATUS='ENABLED' ";


      String c, tn, cn, rtn, rcn;
      st = dbcon.createStatement();
      if (dbcon == null){
        errorMessages = "You must establish a connection first!";
      }
      rs=st.executeQuery(sql);

      while(rs.next()){
        c=rs.getString("c");
        tn=rs.getString("tn");
        cn=rs.getString("cn");
        rtn=rs.getString("rtn");
        rcn=rs.getString("rcn");
        ConstraintRefMetaData crmd = new ConstraintRefMetaData("R", c, tn, cn, rtn, rcn);
        crmd_list.add(crmd);
      }
    }catch(SQLException e){
      System.out.println(errorMessages);
      e.printStackTrace();
    }finally{
      closeDB(dbcon);
    }
    return crmd_list;

  }

  public static void closeDB(Connection dbcon){
    try{
      if (st!=null)
        st.close();
      if (rs!=null)
        rs.close();
      if (dbcon!=null)
        dbcon.close();
    }catch (Exception e){
      errorMessages = "Could not close connection with the Database Server: <br>"+ e.getMessage();
			throw new SQLException(errorMessages);
		}finally{
      return;
    }
  }

}
