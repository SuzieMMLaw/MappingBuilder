package MappingSDK12c;

import java.io.BufferedReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.FileReader;
import java.io.IOException;

public class Configuration{

  //ODI Repository Connection info
  protected static String ODIUrl;
  protected static String ODIDriver;
  protected static String ODIMaster_User;
  protected static String ODIMaster_Pass;
  protected static String ODIWorkRep;
  protected static String Odi_User;
  protected static String Odi_Pass;
  protected static String ODILogin_name;

  //DB Connection info
  protected static String DBurl;
  protected static String DBusername;
  protected static String DBpassword;
  private static String path="Configuration\\Connections.conf";

  public static String getValue(String s){
	  String REGEX="(<.*>)";

	  Pattern p = Pattern.compile(REGEX);
	  Matcher m = p.matcher(s);	
	  String value=null;

	  while(m.find()){
		  value= m.group(0);
	  }
	  
	  value=value.replace("<","");
	  value=value.replace(">","");
	  
	  return value;
  }

	public static void readConf() throws IOException{

		FileReader file_to_read = new FileReader(path);
		BufferedReader bf = new BufferedReader(file_to_read);
		String line;
		String section=null;

		while ((line = bf.readLine()) != null ){
      if (ODIUtils.nameisLike(line, "\\/\\/.*")){
        section=line;
      }

      if (section.equals("//Oracle Data Integrator Connection")){
        if (ODIUtils.nameisLike(line, "Login name.*"))
        	Configuration.ODILogin_name=getValue(line);
        if (ODIUtils.nameisLike(line, "User.*"))
        	Configuration.Odi_User=getValue(line);
        if (ODIUtils.nameisLike(line, "Password.*"))
        	Configuration.Odi_Pass=getValue(line);
      }

      if (section.equals("//Database Connection (Master Repository)")){
        if (ODIUtils.nameisLike(line, "User.*"))
        	Configuration.ODIMaster_User=getValue(line);
        if (ODIUtils.nameisLike(line, "Password.*"))
        	Configuration.ODIMaster_Pass=getValue(line);
        if (ODIUtils.nameisLike(line, "Driver Name.*"))
        	Configuration.ODIDriver=getValue(line);
        if (ODIUtils.nameisLike(line, "URL.*"))
        	Configuration.ODIUrl=getValue(line);
      }
      if (section.equals("//Work Repository")){
        if (ODIUtils.nameisLike(line, "Work Repository.*"))
          Configuration.ODIWorkRep=getValue(line);
      }

      if (section.equals("//Database Connection Information")){
        if (ODIUtils.nameisLike(line, "URL.*"))
        	Configuration.DBurl=getValue(line);
        if (ODIUtils.nameisLike(line, "Username.*"))
        	Configuration.DBusername=getValue(line);
        if (ODIUtils.nameisLike(line, "Password.*"))
        	Configuration.DBpassword=getValue(line);
      }
		}
		bf.close();
		return;
	}
}
