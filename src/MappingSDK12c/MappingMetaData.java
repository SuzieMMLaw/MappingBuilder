package MappingSDK12c;

public class MappingMetaData{
	  private String srcModelName;
	  private String trgModelName;
	  private String trgDatastoreName;
	  private String srcDatastoreName;
	  private String filterCondition;
	  private String mappingName;

	  public MappingMetaData(String mappingName, String srcModelName, String trgModelName, String trgDatastoreName, String srcDatastoreName, String filterCondition){
	    this.mappingName=mappingName;
	    this.srcModelName=srcModelName;
	    this.trgModelName=trgModelName;
	    this.trgDatastoreName=trgDatastoreName;
	    this.srcDatastoreName=srcDatastoreName;
	    this.filterCondition=filterCondition;
	  }

	  public String getSrcModelName(){
	    return this.srcModelName;
	  }

	  public String getTrgModelName(){
	    return this.trgModelName;
	  }

	  public String getTrgDatastoreName(){
	    return this.trgDatastoreName;
	  }

	  public String getSrcDatastoreName(){
	    return this.srcDatastoreName;
	  }

	  public String getFilterCondition(){
	    return this.filterCondition;
	  }
	  public String getMappingName(){
	    return this.mappingName;
	  }
	}
