package MappingSDK12c;

import java.util.*;

public class ConstraintMetaData{
  private String ConstraintType;
  private String ConstraintName;
  private String TableName;
  private String ColName;
  //note to self: to fk den exei list giati einai 1 pros 1
  public ConstraintMetaData(String ConstraintType, String ConstraintName, String TableName, String ColName){
    this.ConstraintType=ConstraintType;
    this.ConstraintName=ConstraintName;
    this.TableName=TableName;
    this.ColName=ColName;
  }


  public String getConstraintType(){
    return this.ConstraintType;
  }

  public String getConstraintName(){
    return this.ConstraintName;
  }

  public String getTableName(){
    return this.TableName;
  }

  public String getColName(){
    return this.ColName;
  }

  public List<String> getColumns(List<ConstraintMetaData> con_meta_list, String ct, String cn){
    List<String> columns=new LinkedList<String>();
    for(ConstraintMetaData cnmd : con_meta_list){
      if(cnmd.getConstraintName().equals(cn) && cnmd.getConstraintType().equals(ct)){
        columns.add(cnmd.getColName());
      }
    }
    return columns;
  }
}

