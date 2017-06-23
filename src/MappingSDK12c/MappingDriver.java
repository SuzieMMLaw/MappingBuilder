package MappingSDK12c;

import java.io.IOException;
import java.util.*;

import oracle.odi.core.*;
import oracle.odi.core.config.*;
import oracle.odi.core.persistence.*;
import oracle.odi.core.persistence.transaction.*;
import oracle.odi.core.persistence.transaction.support.*;
import oracle.odi.core.security.Authentication;
import oracle.odi.domain.project.OdiCKM;
import oracle.odi.domain.project.OdiFolder;
import oracle.odi.domain.project.OdiIKM;
import oracle.odi.domain.model.OdiModel;
import oracle.odi.domain.project.OdiLKM;
import oracle.odi.domain.project.ProcedureOption;
import oracle.odi.domain.project.finder.IOdiCKMFinder;
import oracle.odi.domain.project.finder.IOdiFolderFinder;
import oracle.odi.domain.project.finder.IOdiProjectFinder;
import oracle.odi.domain.topology.finder.IOdiContextFinder;
import oracle.odi.domain.project.finder.IOdiIKMFinder;
import oracle.odi.domain.project.finder.IOdiLKMFinder;
import oracle.odi.domain.model.finder.IOdiModelFinder;
import oracle.odi.domain.model.finder.IOdiKeyFinder;
import oracle.odi.domain.model.OdiDataStore;
import oracle.odi.domain.topology.OdiContext;
import oracle.odi.domain.mapping.*;
import oracle.odi.domain.model.OdiDataStore;
import oracle.odi.domain.model.finder.IOdiDataStoreFinder;
import oracle.odi.domain.mapping.exception.*;
import oracle.odi.domain.adapter.*;
import oracle.odi.domain.model.OdiReference;
import oracle.odi.domain.model.ReferenceColumn;
import oracle.odi.domain.mapping.component.DatastoreComponent;
import oracle.odi.domain.mapping.component.DataStorageDelegate;
import oracle.odi.domain.mapping.*;
import oracle.odi.domain.model.OdiColumn;
import oracle.odi.domain.mapping.MapAttribute;
import oracle.odi.domain.mapping.expression.MapExpression;
import oracle.odi.domain.mapping.physical.MapPhysicalNode;
import oracle.odi.domain.mapping.physical.MapPhysicalDesign;
import oracle.odi.domain.project.OdiProject;


public class MappingDriver{


  private static String target_model_name;
  private static String source_model_name;
  private static String target_datastore;
  private static String source_datastore;
  private static OdiFolder folder;
  private static OdiFolder parentFolder;
  private static OdiContext context;
  private static OdiDataStore sourceDatastore;
  private static OdiDataStore targetDatastore;
  private static OdiModel srcModel;
  private static OdiModel trgModel;
  private static Mapping mapping;
  private static String filter_condition;
  private static String mapping_name;

  public static void main(String[] args) throws IOException {
    //Connection info
    Configuration.readConf();
    String Url = Configuration.ODIUrl;
    String Driver=Configuration.ODIDriver;
    String Master_User=Configuration.ODIMaster_User;
    String Master_Pass=Configuration.ODIMaster_Pass;
    String WorkRep=Configuration.ODIWorkRep;
    String Odi_User=Configuration.Odi_User;
    String Odi_Pass=Configuration.Odi_Pass;
    String Login_name=Configuration.ODILogin_name;

    List<MappingMetaData> map_meta_list =DBUtils.getMappingInfo();
    for (MappingMetaData m : map_meta_list){

      source_datastore=m.getSrcDatastoreName();
      target_model_name = m.getTrgModelName();
      source_model_name = m.getSrcModelName();
      target_datastore=m.getTrgDatastoreName();
      filter_condition=m.getFilterCondition();
      mapping_name=m.getMappingName();

      //Create connection
      MasterRepositoryDbInfo masterInfo = new MasterRepositoryDbInfo(Url, Driver, Master_User, Master_Pass.toCharArray(), new PoolingAttributes());
      WorkRepositoryDbInfo workInfo = new WorkRepositoryDbInfo(WorkRep, new PoolingAttributes());
      OdiInstance odiInstance=OdiInstance.createInstance(new OdiInstanceConfig(masterInfo,workInfo));
      Authentication auth = odiInstance.getSecurityManager().createAuthentication(Odi_User, Odi_Pass.toCharArray());
      odiInstance.getSecurityManager().setCurrentThreadAuthentication(auth);
      ITransactionStatus trans = odiInstance.getTransactionManager().getTransaction(new DefaultTransactionDefinition());

      System.out.println("Begin process for new mapping: " + m.getMappingName());
      DBUtils.updateMappingStatus("IN PROCESS", m.getMappingName());


      //Find and set context
      context = ((IOdiContextFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiContext.class)).findByCode(ODIUtils.Context_Code);


      //Find the 3NF model aka source model (for now)
      srcModel =  ((IOdiModelFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiModel.class)).findByCode(source_model_name);
      if(srcModel!=null){
        System.out.println("Source Model \""+source_model_name+"\" was found");
      }else{
        System.out.println("Error: Source Model \""+source_model_name+"\"  was not found");
      }
      //Find the 3NF model aka target model and initialize model objects and attributes
      trgModel =  ((IOdiModelFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiModel.class)).findByCode(target_model_name);
      if(trgModel!=null){
        System.out.println("Target Model \""+target_model_name+"\" was found");
      }else{
        System.out.println("Error: Target Model \""+target_model_name+"\" was not found");

      }

      //Check if datastores exist, else reverseEngineer them
      //Find my existing datastores in OCDM_3NF model, and create OdiDataStore objects
      sourceDatastore = ((IOdiDataStoreFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(source_datastore, source_model_name);

      if (sourceDatastore!=null){
        System.out.println("Source Datastore was found: " + sourceDatastore.toString());
      }else{
        sourceDatastore = ODIUtils.reverseEngineer(odiInstance, source_datastore, srcModel);
         System.out.println("New source Datastore was created: "+ sourceDatastore.toString());
      }

      odiInstance.getTransactionalEntityManager().persist(sourceDatastore);


      targetDatastore = ((IOdiDataStoreFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiDataStore.class)).findByName(target_datastore, target_model_name);

      if (targetDatastore!=null){
       System.out.println("Target Datastore was found: "+ targetDatastore.toString());
      }else{
       targetDatastore = ODIUtils.reverseEngineer(odiInstance, target_datastore, trgModel);
        System.out.println("New target Datastore was created: "+ targetDatastore.toString());
      }
      odiInstance.getTransactionalEntityManager().persist(targetDatastore);

      //Find existing project and parent folder
      OdiProject proj = ((IOdiProjectFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiProject.class)).findByCode(ODIUtils.Project_Code);
      Collection<OdiFolder> parentf=((IOdiFolderFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiFolder.class)).findByName(ODIUtils.parent_proj_folder);
      for (Iterator<OdiFolder> it = parentf.iterator(); it.hasNext();) {
        parentFolder = it.next();
      }

      //Check if SDK_subfolder already exists and if not create it under parent folder
      Collection<OdiFolder> subf=((IOdiFolderFinder) odiInstance.getTransactionalEntityManager().getFinder(OdiFolder.class)).findByName(ODIUtils.folder_name, ODIUtils.Project_Code);
      for (Iterator<OdiFolder> it = subf.iterator(); it.hasNext();) {
        folder = it.next();
        System.out.println("Folder exists: "+folder.toString());
      }

      if (folder==null){
        folder = new OdiFolder(parentFolder, ODIUtils.folder_name);
        System.out.println("New folder was created: " + folder.toString() );
      }

      odiInstance.getTransactionalEntityManager().persist(folder);

      //Create mapping
      mapping = ODIUtils.createMappingLKP(odiInstance, mapping_name, folder, sourceDatastore, targetDatastore, filter_condition);

      // Finally commit and close the Instance
      odiInstance.getTransactionManager().commit(trans);
      odiInstance.close();
      System.out.println("New mapping process completed");

    }//end for
    System.out.println("Exit..." );
  }
}
