package MappingSDK12c;
import java.util.*;
import oracle.odi.core.OdiInstance;
import oracle.odi.domain.project.OdiFolder;
import oracle.odi.domain.project.OdiIKM;
import oracle.odi.domain.model.OdiModel;
import oracle.odi.domain.project.finder.IOdiIKMFinder;
import oracle.odi.domain.model.OdiDataStore;
import oracle.odi.domain.model.finder.IOdiDataStoreFinder;
import oracle.odi.domain.mapping.exception.*;
import oracle.odi.domain.adapter.*;
import oracle.odi.domain.model.OdiReference;
import oracle.odi.domain.model.ReferenceColumn;
import oracle.odi.domain.mapping.component.DatastoreComponent;
import oracle.odi.domain.mapping.Mapping;
import oracle.odi.domain.mapping.finder.IMappingFinder;
import oracle.odi.domain.model.OdiColumn;
import oracle.odi.domain.mapping.MapAttribute;
import oracle.odi.domain.mapping.expression.MapExpression;
import oracle.odi.domain.mapping.physical.MapPhysicalNode;
import oracle.odi.domain.mapping.physical.MapPhysicalDesign;
import oracle.odi.domain.mapping.component.FilterComponent ;
import oracle.odi.domain.model.OdiKey;
import java.util.regex.Matcher;
import java.util.regex.Pattern;






public class ODIUtils{



  public static String Project_Code="FORTHNET_OCDM_PROJECT";
  public static String folder_name="AUTOMATED_LKP_MAPPINGS";
  public static String Context_Code="FORTHNET_CONTEXT";
  public static String parent_proj_folder="Customer Management Area";


  //private static String LKMname = "LKM SQL to Oracle";;
  private static String IKMname = "IKM Oracle Incremental Update (MERGE)";
  //private static String CKMname = "CKM Oracle";
  //private static String IKMname = "IKM Oracle Control Append";
  private static Mapping mapping;

  public static Mapping createMappingLKP(OdiInstance odiInstance, String mappingName, OdiFolder folder, OdiDataStore  sourceDatastore, OdiDataStore targetDatastore, String filter_condition){
    try{

        if (((IMappingFinder) odiInstance.getTransactionalEntityManager().getFinder(Mapping.class)).findByName(folder, mappingName)!=null){
          System.out.println("A mapping with the name: " + mappingName+ " under the folder: "+ folder.toString()+" already exists. You have to pick a different name.");
          System.out.println("Hint: make sure that the STATUS field in MPNGS_MTDT is updated to \"COMPLETE\"");
          DBUtils.updateMappingStatus("FAILED", mappingName);
          return null;
        }

        mapping = new Mapping(mappingName, folder);


        odiInstance.getTransactionalEntityManager().persist(folder);

        DatastoreComponent comp_srcds = new DatastoreComponent(mapping, sourceDatastore);
        DatastoreComponent comp_trgds = new DatastoreComponent(mapping, targetDatastore);

        List<MapAttribute> satts = comp_srcds.getAttributes();
        List<MapAttribute> tatts = comp_trgds.getAttributes();

        ListIterator<MapAttribute> iter = tatts.listIterator();


        if (filter_condition!=null){
          FilterComponent filter = new FilterComponent(mapping, "FILTER1");
          //filter_condition = String fcond="TEST_SRC_SDK.ATTR_1>1";
          if (!nameisLike(filter_condition, "^.*[.].*$")){
            filter_condition=sourceDatastore.getName()+"."+filter_condition;
          }
          filter.setFilterCondition(filter_condition);
          comp_srcds.connectTo(filter);
        }

        //public MapExpression(IMapExpressionOwner expressionOwner,java.lang.String text) throws MappingException
        for (MapAttribute trg_attr : tatts){

          if (nameisLike(trg_attr.getRefName(),".*_KEY$")){
            trg_attr.setExpression(new MapExpression(trg_attr, targetDatastore.getName()+"_SEQ.NEXTVAL"));
            trg_attr.setExecuteOnHint(MapExpression.ExecuteOnLocation.valueOf("TARGET"));
            trg_attr.setActive(true);
            trg_attr.setInsertIndicator(true);
            trg_attr.setUpdateIndicator(false);
            trg_attr.setCheckNotNullIndicator(false);
          }

          if(nameisLike(trg_attr.getRefName(), ".*LOAD_DT$")){
            trg_attr.setExpression(new MapExpression(trg_attr,"SYSDATE"));
            trg_attr.setExecuteOnHint(MapExpression.ExecuteOnLocation.valueOf("TARGET"));
            trg_attr.setActive(true);
            trg_attr.setInsertIndicator(true);
            trg_attr.setUpdateIndicator(false);
            trg_attr.setCheckNotNullIndicator(false);

          }
          if(nameisLike(trg_attr.getRefName(), ".*LAST_UPDT_DT$")){
            trg_attr.setExpression(new MapExpression(trg_attr,"SYSDATE"));
            trg_attr.setExecuteOnHint(MapExpression.ExecuteOnLocation.valueOf("TARGET"));
            trg_attr.setActive(true);
            trg_attr.setInsertIndicator(true);
            trg_attr.setUpdateIndicator(true);
            trg_attr.setCheckNotNullIndicator(false);

          }

          if (nameisLike(trg_attr.getRefName(), ".*LAST_UPDT_BY$")){
            trg_attr.setExpression(new MapExpression(trg_attr,"'ODI'"));
            trg_attr.setExecuteOnHint(MapExpression.ExecuteOnLocation.valueOf("TARGET"));
            trg_attr.setActive(true);
            trg_attr.setInsertIndicator(true);
            trg_attr.setUpdateIndicator(true);
            trg_attr.setCheckNotNullIndicator(false);

          }

          if(nameisLike(trg_attr.getRefName(), ".*_CD$")){
            trg_attr.setExpression(getSrcAttribute(satts,".*_CD$"));
          }

          if( nameisLike(trg_attr.getRefName(), ".*_DESC$") || nameisLike(trg_attr.getRefName(), ".*_NAME$")){
            trg_attr.setExpression(getSrcAttribute(satts,".*_VALUE$"));
          }
        }
        
        OdiKey key=null;
        // = ((IOdiKeyFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiKey.class)).findByName(targetDatastore.getName()+"_UK", targetDatastore.getDataStoreId());
        Collection<OdiKey> keys = targetDatastore.getKeys();
        for (Iterator<OdiKey> it = keys.iterator(); it.hasNext();){
          key =it.next();
          if (nameisLike(key.getName(), ".*_UK$")){
            comp_trgds.setUpdateKey(key);
            System.out.println(key.getName()+" was set as UK");
            break;
          }
        }



        //Create physical design of the mapping                         ~~~~~ MapPhysicalDesign phys_design = new MapPhysicalDesign(mapping, "load_trg_sdk_map_physical", context, true,true, null);
        MapPhysicalDesign phys_design = mapping.createPhysicalDesign(mappingName+"_physical");

        phys_design.setOptimizationContextByCode(Context_Code);
        //Find and set IKM in the target_source_datastore
        //find all imported IKMs in the project
        Collection<OdiIKM> IKM_col = ((IOdiIKMFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiIKM.class)).findByName(IKMname, Project_Code);
        Iterator<OdiIKM> it=IKM_col.iterator();
        OdiIKM IKM = it.next();
        if (IKM==null){
            System.out.println("IKM was not found!!! You have to import IKMs first! ");
        }else{
            System.out.println("IKM found: "+ IKM.toString());


            MapPhysicalNode phys_node_trg= phys_design.findNode(comp_trgds);
            
            if(phys_design==null){
              System.out.println("Physical design was not created");
            }else{
              System.out.println("Physical design is: "+phys_design.toString());
            }

            if(phys_node_trg==null) System.out.println("Physical node was not found");
            phys_node_trg.setIKM(IKM);

            //get and reset the value (IOptionValue) of the IKM for this particular physical node (aka our target datastore)
            try{
                (phys_node_trg.getIKMOptionValue("SYNC_JRN_DELETE")).setValue(false);
                (phys_node_trg.getIKMOptionValue("FLOW_CONTROL")).setValue(true);
            }catch(Exception ex){
                System.out.print(ex.getMessage());
            }
        }

      }
      catch(AdapterException ae){
        DBUtils.updateMappingStatus("FAILED",mappingName);
        System.out.println(ae.getMessage());
      }
      catch(MappingException me){
        DBUtils.updateMappingStatus("FAILED",mappingName);
        System.out.println(me.getMessage());
      }
      finally{
        if (mapping!=null){
          System.out.println("New mapping created with name: " +mapping.toString());
          DBUtils.updateMappingStatus("COMPLETE", mappingName);
        }else{
          System.out.println("Mapping creation failed");
          DBUtils.updateMappingStatus("FAILED",mappingName);
        }
      }
    return mapping;
  }

  public static OdiDataStore reverseEngineer(OdiInstance odiInstance, String table, OdiModel model){

    //check table metadata with dbconn method List<ColumnMetaData> getTableMetadata(table, owner)
    String owner="";
    if((model.getName()).equals("OCDM_3NF")){
      owner="OCDM_SYS";
    }else if((model.getName()).equals("OCDM_STAGE")){
      owner="OCDM_ODI_STAGE";
    }


    OdiDataStore datastore=new OdiDataStore(model, table);

    datastore.setDefaultAlias(table);
    List<ColumnMetaData> col_meta_list =DBUtils.getColMetaData(table, owner);

    for (ColumnMetaData cmd : col_meta_list) {
      OdiColumn col = new OdiColumn(datastore, cmd.getColumnName());
      col.setDataTypeCode(cmd.getDatatype());
      col.setMandatory(!cmd.getNullable());
      if (!nameisLike(cmd.getDatatype(), "DATE")){
        col.setLength(cmd.getLength());
        col.setScale(cmd.getScale());
      }
    }


    //for uk and pk
    List<ConstraintMetaData> con_meta_list = DBUtils.getConMetaData(table, owner);
    OdiKey k=null;
    List<String> constraints_added = new LinkedList<String>();
    for(ConstraintMetaData cnmd : con_meta_list){
      //check if key already exist, else create new one
      //System.out.println("Checking if key: "+cnmd.getConstraintName()+" already exists");
      if (constraints_added.contains(cnmd.getConstraintName())){
        System.out.println("Key "+cnmd.getConstraintName()+" exists");
      }else{
        System.out.println("Key "+cnmd.getConstraintName()+" was not found... Creating it now");
        k = new OdiKey(datastore, cnmd.getConstraintName());

        if (cnmd.getConstraintType().equals("P"))//don't have to check if else for UK because key_type is set to ALTERNATE_KEY by default
          k.setKeyType(OdiKey.KeyType.valueOf("PRIMARY_KEY"));//OdiKey.KeyType keyType = OdiKey.KeyType.valueOf("PRIMARY_KEY");sdkPrimaryKey.setKeyType(keyType);

        constraints_added.add(cnmd.getConstraintName());
      }
      k.addColumn(datastore.getColumn(cnmd.getColName()));

    }

    //check constraints metadata and add fk

    List<ConstraintRefMetaData> con_ref_meta_list = DBUtils.getConRefMetaData(table, owner);
    for(ConstraintRefMetaData crmd : con_ref_meta_list){

      OdiDataStore refdatastore =  ((IOdiDataStoreFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(crmd.getRefTableName(), model.getName());

      if (refdatastore==null){
        System.out.println("The reference datastore cannot be found anywhere in ODI... Creating it now...");
        refdatastore = reverseEngineer(odiInstance, crmd.getRefTableName(), model);
      }

      OdiReference r = new OdiReference(datastore, refdatastore, crmd.getConstraintName());
      r.setReferenceType(OdiReference.ReferenceType.valueOf("DB_REFERENCE"));
      String c = crmd.getColName();
      ReferenceColumn rc = new ReferenceColumn(r, datastore.getColumn(c), refdatastore.getColumn(crmd.getRefColName()));
    }
        odiInstance.getTransactionalEntityManager().persist(datastore);

    return datastore;
  }

  public static boolean nameisLike(String input, String regex){
    Pattern p = Pattern.compile(regex);
    Matcher m = p.matcher(input);   // get a matcher object
    return m.matches();
  }


  public static MapAttribute getSrcAttribute(List<MapAttribute> list, String regex){

      for (MapAttribute m : list){
        if (nameisLike(m.getRefName(), regex)) return m;
      }
      return null;

  }

}
