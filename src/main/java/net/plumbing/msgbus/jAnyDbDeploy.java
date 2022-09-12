package net.plumbing.msgbus;
//=====================================================================
//  File:    jAnyDbDeploy.java
//  Summary:
//=====================================================================

import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageType;
import org.apache.commons.cli.*;
import net.plumbing.msgbus.init.InitTemplates_InitTypes;

import java.io.*;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;

public class jAnyDbDeploy {
//	private static  String HrmsSchema;
	static public Connection Oracle_Connection = null;
	static public String connectionUrl ;
	static private String ora_point;


	// public static final Logger PerformTemplates_Log = LoggerFactory.getLogger(jAnyDbDeploy.class);
	public static String messageDirection=null;
	public static String sInterface_Id=null;
	public static String sOperation_Id=null;
	public static String Msg_Type=null;
	public static String TemplatePart=null;

	static private void make_Oracle_Connection( String db_userid, String db_password ) {

		/*if ( ora_point==null) {
			//connectionUrl = "jdbc:oracle:thin:@//10.10.1.1:7016/hermes"; // Бой !!!
			// connectionUrl = "jdbc:oracle:thin:@//10.32.245.10:1521/hermes12"; // Test !!!
			connectionUrl = "jdbc:oracle:thin:@//localhost:15021/ORCLPDB1";
		}
		else {
			connectionUrl = "jdbc:oracle:thin:@//"+ora_point;
		}*/

		if ( ora_point==null) {
			connectionUrl = "jdbc:oracle:thin:@//10.242.36.8:1521/hermes12"; // Test-Capsul !!!
			//connectionUrl = "jdbc:oracle:thin:@//10.32.245.4:1521/hermes"; // Бой !!!
		}
		else {
			//connectionUrl = "jdbc:oracle:thin:@"+dst_point;
			connectionUrl = ora_point;
		}

		String ClassforName;
		if ( connectionUrl.indexOf("oracle") > 0 )
			ClassforName = "oracle.jdbc.driver.OracleDriver";
		else ClassforName = "org.postgresql.Driver";

		// String db_userid = HrmsSchema; //"artx_proj";
		//String db_password = "ormid"; // ""rIYmcN38St5P";
		System.out.println( "Try DataBase getConnection: "  + connectionUrl + " as " + db_userid + " password[" + db_password + "] , Class.forName:" + ClassforName);
		try {
			// Establish the connection.
			Class.forName(ClassforName);
			Oracle_Connection = DriverManager.getConnection(connectionUrl, db_userid, db_password);
			// Handle any errors that may have occurred.
			Oracle_Connection.setAutoCommit(false);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}


	public static void main(String[] args)  throws Exception {
		java.util.Properties m_properties = System.getProperties();
		ora_point = m_properties.getProperty("ora.point");
		//is_verbose = m_properties.getProperty("is.verbose");
		Options options = new Options();

		Option input = new Option("i", "input", true, "input file path");
		input.setRequired(true);
		options.addOption(input);

		Option output = new Option("o", "output", true, "output file");
		output.setRequired(true);
		options.addOption(output);

		Option schema = new Option("s", "schema", true, "database schema(user owner) name");
		output.setRequired(true);
		options.addOption(schema);

		Option password = new Option("p", "password", true, "database user password");
		output.setRequired(true);
		options.addOption(password);

		CommandLineParser parser = new DefaultParser();
		HelpFormatter formatter = new HelpFormatter();
		CommandLine cmd;
		String inputFilePath=null;
		String outputFilePath=null;
		String hrmsSchema="orm";
		String hrmsPassword="ormid";

		try {
			cmd = parser.parse(options, args);
			inputFilePath = cmd.getOptionValue("input");
			outputFilePath = cmd.getOptionValue("output");
			hrmsSchema = cmd.getOptionValue("schema");
			hrmsPassword = cmd.getOptionValue("password");

		} catch (ParseException e) {
			System.out.println(e.getMessage());
			formatter.printHelp("utility-name", options);

			System.exit(1);
		}


		System.out.println("inputFilePath:" + inputFilePath);
		System.out.println("outputFilePath:" + outputFilePath);
		System.out.println("database schema name:" + hrmsSchema);
		// HrmsSchema = hrmsSchema;
		String File_separator =  File.separator;

		File f = new File(inputFilePath);
        System.out.println("File_separator:" + File_separator );
        System.out.println("getAbsolutePath:" + f.getAbsolutePath());
		String inputFileName= f.getAbsolutePath().substring(f.getAbsolutePath().lastIndexOf(File_separator)+1);

		parceInputfileName(inputFileName);
		System.out.println(TemplatePart );
		String messageTemplateConf_Text =
		jAnyDbDeploy.readUsingOutputStream( inputFilePath, null);
		if ( messageTemplateConf_Text == null)
			System.exit(2);
		try {
			// String SQL = "SELECT TOP 5 * FROM Persons_fired_VV";
;
 		    make_Oracle_Connection( hrmsSchema, hrmsPassword );
			System.out.println( "Try InitTemplates_InitTypes.SelectMsgTemplates( "
					+ messageDirection + ", "
					+ Integer.parseInt(sInterface_Id) + ", "
					+ Integer.parseInt(sOperation_Id) + ")");
		}
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
			System.exit(3);
		}

		int numMessageTypeFound = // Зачитывем ТИП
				Perform.getMessageTypeVO( hrmsSchema, sInterface_Id, sOperation_Id);
		// Проверяем наличие Типа сообщения
		if ( numMessageTypeFound != 1)
		{
			System.err.println("В выборке по ["
					+ " and t.Interface_Id = " + sInterface_Id
					+ " and t.Operation_Id = " + sOperation_Id
					+ " ] не нашли тип сообщения");
			System.exit(-50 - numMessageTypeFound);
		}

		int numTemplatefound = // Зачитывем, с учетом IN|OUT ARTX_PROJ.MESSAGE_typeS.msg_direction из Perform.getMessageTypeVO() !
		    Perform.getMessageTemplateVO(hrmsSchema, messageDirection, sInterface_Id, sOperation_Id, MessageType.AllMessageType.get(0).getMsg_Direction() );
		int Template_Id_INSERT_MessageTemplate=0;
		if ( numTemplatefound < 0 )
			System.exit(numTemplatefound);
		if ( numTemplatefound > 1  ) {
			System.err.println("В выборке по ["
							+ " and " + hrmsSchema + ".x_templates.look_2_MessageDirections(t.destin_id, t.dst_subcod) = '" + messageDirection + "'"
							+ " and t.Interface_Id = " + sInterface_Id
							+ " and t.Operation_Id = " + sOperation_Id
					        + " ]больше 1-го шаблона");
			System.exit(-100 - numTemplatefound);
		}
		if ( numTemplatefound == 1 ) {
			System.out.println("Conf_Text() init:\n" + MessageTemplate.AllMessageTemplate.get(0).getConf_Text());
			Template_Id_INSERT_MessageTemplate = MessageTemplate.AllMessageTemplate.get(0).getTemplate_Id();
		}
		if ( numTemplatefound == 0 )
		{ // Если шаблон не найден, то его создаем по типу

			Perform.newMessageTemplateVO( hrmsSchema,
					MessageType.AllMessageType.get(0), messageDirection
			);

			System.out.println( "getLastMaker():\n" + MessageTemplate.AllMessageTemplate.get(0).getLastMaker() );
			//System.exit(96);
			// Добавляем шаблон в БД
			Template_Id_INSERT_MessageTemplate =
			InitTemplates_InitTypes.doINSERT_MessageTemplate( hrmsSchema,MessageTemplate.AllMessageTemplate.get(0) );
			if ( Template_Id_INSERT_MessageTemplate < 0)
				System.exit(-60);
		}
        System.out.println( "Conf_Text() before:\n" + MessageTemplate.AllMessageTemplate.get(0).getConf_Text() );
		String Conf_Text =
		Perform.replace_Conf_Text( hrmsSchema, MessageTemplate.AllMessageTemplate.get(0), TemplatePart, messageTemplateConf_Text);
        System.out.println( "Conf_Text() for update [Template_Id=" + Template_Id_INSERT_MessageTemplate + "]:\n" + Conf_Text );
		int result_UPDATE_MessageTemplate =
				InitTemplates_InitTypes.doUpdate_MESSAGE_Template( hrmsSchema, Template_Id_INSERT_MessageTemplate, Conf_Text
				);
if ( result_UPDATE_MessageTemplate == 0 )
	System.err.println("update "+ hrmsSchema + ".Message_Template for Template_Id =" + Template_Id_INSERT_MessageTemplate + " has been successfully done.");
		System.exit(result_UPDATE_MessageTemplate);
	}

	private static void parceInputfileName(String inputFileName ) {
		System.out.println(inputFileName);
		int indexS = inputFileName.indexOf("._"); // Нахождение символа в строке
		int indexE = inputFileName.indexOf("_."); // Нахождение символа в строке
		if (( indexS > 3 ) && ( indexE > 5)) {
			messageDirection = inputFileName.substring( indexS +2 ,indexE );
		}
		else {
			System.err.println(" Не найден код подсистемы для секции шаблона в имени файла " + inputFileName);
			System.exit(1);
		}
		System.out.println(messageDirection);

		indexS = inputFileName.indexOf("."); // Нахождение символа в строке
		if ( indexS >= 1 ) {
			sInterface_Id = inputFileName.substring( 0, indexS  );
		}
		else {
			System.err.println(" Не найден номер интерфейса - первые цифры до \".\" в имени файла " + inputFileName);
			System.exit(2);
		}
		System.out.println(sInterface_Id);

		indexS = inputFileName.indexOf(".BO_"); // Нахождение символа в строке
		indexE = inputFileName.indexOf("._"); // Нахождение символа в строке
		if (( indexS >= 1 ) && ( indexE > 4)) {
			sOperation_Id = inputFileName.substring( indexS +4 ,indexE );
		}
		else {
			System.err.println("indexS=" + indexS + ", indexE=" + indexE);

			System.err.println(" Не найден номер операции -  цифры после \".BO_\"  и перед \"._\" в имени файла " + inputFileName);
			System.exit(1);
		}
		System.out.println(sOperation_Id);

		indexS = inputFileName.indexOf("_."); // Нахождение символа в строке

		//System.err.println("pre Msg_Type: indexS=" + indexS + ", indexE=" + indexE);
		if ( indexS > 5 )   {
			indexE = inputFileName.substring( indexS +2).indexOf("."); // Нахождение символа в строке
			if( indexE > 1) {

				Msg_Type = inputFileName.substring(indexS + 2, indexS +2 + indexE);
			}
			else {
				System.err.println("Msg_Type: indexS=" + indexS + ", indexE=" + indexE +  " inputFileName.substring:" + inputFileName.substring(indexS + 2) );
				System.exit(1);
			}
		}
		else {
			System.err.println("Msg_Type: > indexS=" + indexS + ", indexE=" + indexE);

			System.err.println(" Не найден тип операции - между \".\"  и перед \".\" в имени файла после " + inputFileName);
			System.exit(1);
		}
		System.out.println("Msg_Type:" + Msg_Type);

		TemplatePart = inputFileName.substring(indexS +2 + indexE+1);
		indexE = TemplatePart.indexOf("."); // Нахождение символа в строке
		if( indexE > 1) {

			TemplatePart = TemplatePart.substring(0,  indexE);
		}
		else {
			System.err.println("TemplatePart:  indexE=" + indexE   + "TemplatePart.substring:" + TemplatePart );
			System.exit(1);
		}
		System.out.println("TemplatePart:" + TemplatePart);
	}

	private static String readUsingOutputStream( String fullFilePath, FileTime time) {
		InputStream os = null;
		String data = null;
		File tFile = null;
		tFile = new File(fullFilePath);
		Long Filelength = tFile.length();


		if ( tFile == null ) return null;
		byte RowBytes[] = new byte[ Filelength.intValue() ]; // =  data.getBytes("UTF-8");
		try {
			tFile = new File(fullFilePath);
			tFile.length();
			os = new FileInputStream( tFile );

			os.read(RowBytes , 0, Filelength.intValue());
			data = new String(RowBytes, "UTF-8");
			// System.out.println("data: " + data);
		} catch (IOException e) {
			System.err.println( "read from (" + fullFilePath + ") failed: [" + e.getMessage() + "]");
			// e.printStackTrace();
		}finally{
			try {
				if ( os != null ) {
					{
						os.close();
					}
					//setFileTime(fullFilePath, time);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println( "close " + fullFilePath + " failed:" + e.getMessage());
			}
			return data;
		}
	}


/*
	private static void writeUsingOutputStream(String data, String fullFilePath, FileTime time) {
		OutputStream os = null;
		if ( data == null ) return;
		try {
			os = new FileOutputStream(new File(fullFilePath));
			byte RowBytes[] =  data.getBytes("UTF-8");
			os.write(RowBytes , 0, RowBytes.length);
		} catch (IOException e) {
			System.err.println( "write to " + fullFilePath + " failed:" + e.getMessage());
			e.printStackTrace();
		}finally{
			try {
				if ( os != null ) {
					{   os.flush();
						os.close();
					}
					//setFileTime(fullFilePath, time);
				}
			} catch (IOException e) {
				e.printStackTrace();
				System.err.println( "close " + fullFilePath + " failed:" + e.getMessage());
			}
		}
	}
*/
}

