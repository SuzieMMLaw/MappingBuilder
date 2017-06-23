package MappingSDK12c;

public class ColumnMetaData{
	  private String TableName;
	  private String ColumnName;
	  private String datatype;
	  private Integer length;
	  private Integer scale;
	  private boolean nullable;


	  public ColumnMetaData (String TableName, String ColumnName, String datatype, Integer length, Integer scale, boolean nullable ){
	    this.TableName=TableName;
	    this.ColumnName=ColumnName;
	    this.datatype=datatype;
	    if(length<=0){
	      this.length=null;
	    }else{
	      this.length=length;
	    }
	    if(scale<=0){
	      this.scale=null;
	    }else{

	      this.scale=scale;
	    }
	    this.nullable=nullable;

	  }

	  public String getTableName(){
	    return this.TableName;
	  }

	  public String getColumnName(){
	    return this.ColumnName;
	  }

	  public String getDatatype(){
	    return this.datatype;
	  }

	  public Integer getLength(){
	    return this.length;
	  }

	  public Integer getScale(){
	    return this.scale;
	  }

	  public boolean getNullable(){
	    return this.nullable;
	  }
	}

