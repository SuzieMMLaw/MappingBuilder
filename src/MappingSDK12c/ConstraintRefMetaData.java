package MappingSDK12c;

public class ConstraintRefMetaData extends ConstraintMetaData{
	  private String refTableName;
	  private String refColName;


	  public ConstraintRefMetaData(String ConstraintType, String ConstraintName, String TableName, String ColName, String refTableName, String refColName){
	    super(ConstraintType, ConstraintName, TableName, ColName);
	    this.refColName=refColName;
	    this.refTableName=refTableName;
	  }


	  public String getRefTableName(){
	    return this.refTableName;
	  }

	  public String getRefColName(){
	    return this.refColName;
	  }

	}
