package net.plumbing.msgbus;

import net.plumbing.msgbus.model.MessageTemplate;
import net.plumbing.msgbus.model.MessageTemplateVO;
import net.plumbing.msgbus.model.MessageTypeVO;
import net.plumbing.msgbus.init.ConfigMsgTemplates;
import net.plumbing.msgbus.init.InitTemplates_InitTypes;
import net.plumbing.msgbus.model.MessageDirections;

import static net.plumbing.msgbus.common.XMLchars.*;

public class Perform {

private static  int appendStringBuffer( String configContent, StringBuffer pTemplConfig, String configEntry )
{
    if ( configContent != null) {
        pTemplConfig.append(OpenTag);
        pTemplConfig.append(configEntry);
        pTemplConfig.append(CloseTag);
        pTemplConfig.append(CDATAopen);
        pTemplConfig.append(configContent);
        pTemplConfig.append(CDATAclose);
        pTemplConfig.append(OpenTag);
        pTemplConfig.append(EndTag);
        pTemplConfig.append(configEntry);
        pTemplConfig.append(CloseTag);
        return 1;
    }
return 0;
}
public static  String Conf_Text_Begin= "<?xml version='1.0' encoding='utf-8'?><TemplConfig>";
    public static  String Conf_Text_End="</TemplConfig>";

    static public String replace_Conf_Text(MessageTemplateVO messageTemplateVO, String configEntry, String configContent) throws Exception {
        StringBuffer TemplConfig = new StringBuffer(Conf_Text_Begin);

        System.out.println( "configEntry:" + configEntry // + "\n configContent:" + configContent
        );
        switch ( configEntry) {
            case "EnvelopeInXSLT":
                messageTemplateVO.setEnvelopeInXSLT(configContent);
                break;

            case "HeaderInXSLT":
                messageTemplateVO.setHeaderInXSLT(configContent);
                break;
            case "WsdlInterface":
                messageTemplateVO.setWsdlInterface(configContent);
                break;
            case "WsdlXSD1":
                messageTemplateVO.setWsdlXSD1(configContent);
                break;
            case "WsdlXSD2":
                messageTemplateVO.setWsdlXSD2(configContent);
                break;
            case "WsdlXSD3":
                messageTemplateVO.setWsdlXSD3(configContent);
                break;
            case "WsdlXSD4":
                messageTemplateVO.setWsdlXSD4(configContent);
                break;
            case "WsdlXSD5":
                messageTemplateVO.setWsdlXSD5(configContent);
                break;
            case "WsdlXSD6":
                messageTemplateVO.setWsdlXSD6(configContent);
                break;
            case "WsdlXSD7":
                messageTemplateVO.setWsdlXSD7(configContent);
                break;
            case "WsdlXSD8":
                messageTemplateVO.setWsdlXSD8(configContent);
                break;
            case "WsdlXSD9":
                messageTemplateVO.setWsdlXSD9(configContent);
                break;

            case "ConfigExecute":
                messageTemplateVO.setConfigExecute( configContent );
                break;
            case "MessageXSD":
                messageTemplateVO.setMessageXSD( configContent);
                break;
            case "HeaderXSLT":
                messageTemplateVO.setHeaderXSLT( configContent);
                break;

            case  "ConfigPostExec":
                messageTemplateVO.setConfigPostExec( configContent);
                break;
            case "EnvelopeXSLTPost":
                messageTemplateVO.setEnvelopeXSLTPost( configContent);
                break;

            case "MsgAnswXSLT":
                messageTemplateVO.setMsgAnswXSLT( configContent);
                break;

            case "MessageXSLT":
                messageTemplateVO.setMessageXSLT( configContent);
                break;

            case "AckXSLT":
                messageTemplateVO.setAckXSLT( configContent);
                break;

            case "EnvelopeXSLTExt":
                messageTemplateVO.setEnvelopeXSLTExt( configContent);
                break;

            case "EnvelopeNS":
                messageTemplateVO.setEnvelopeNS( configContent);
                break;
            case "MessageAck":
                messageTemplateVO.setMessageAck( configContent);
                break;
            case "MessageAnswAck":
                messageTemplateVO.setMessageAnswAck( configContent);
                break;
            case "MessageAnswerXSD":
                messageTemplateVO.setMessageAnswerXSD( configContent);
                break;
            case "MessageAnswMsgXSLT":
                messageTemplateVO.setMessageAnswMsgXSLT ( configContent);
                break;
            case "AckXSD":
                messageTemplateVO.setAckXSD( configContent);
                break;
            case "AckAnswXSLT":
                messageTemplateVO.setAckAnswXSLT( configContent);
                break;
            case "HeaderXSD":
                messageTemplateVO.setHeaderXSD( configContent);
                break;
            case "ErrTransXSLT":
                messageTemplateVO.setErrTransXSLT( configContent);
                break;
            default:
                throw  new java.lang.Exception("Unknown \"configEntry:\"=[" + configEntry+ "]");
        }


        System.out.println("PerformTemplates, пишем в (" + configEntry + ") [" + configContent + "]");
        appendStringBuffer( messageTemplateVO.getHeaderInXSLT(), TemplConfig , "HeaderInXSLT" );
        appendStringBuffer( messageTemplateVO.getWsdlInterface(), TemplConfig , "WsdlInterface"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD1(), TemplConfig , "WsdlXSD1"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD2(), TemplConfig , "WsdlXSD2"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD3(), TemplConfig , "WsdlXSD3"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD4(), TemplConfig , "WsdlXSD4"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD5(), TemplConfig , "WsdlXSD5"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD6(), TemplConfig , "WsdlXSD6"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD7(), TemplConfig , "WsdlXSD7"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD8(), TemplConfig , "WsdlXSD8"  );
        appendStringBuffer( messageTemplateVO.getWsdlXSD9(), TemplConfig , "WsdlXSD9"  );


        appendStringBuffer( messageTemplateVO.getMessageXSD(), TemplConfig , "MessageXSD"  );
        appendStringBuffer( messageTemplateVO.getMessageXSLT(), TemplConfig , "MessageXSLT"  );
        appendStringBuffer( messageTemplateVO.getAckXSLT(), TemplConfig , "AckXSLT"  );
        appendStringBuffer( messageTemplateVO.getMsgAnswXSLT(), TemplConfig , "MsgAnswXSLT"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeXSLTPost(), TemplConfig , "EnvelopeXSLTPost"  );
        appendStringBuffer( messageTemplateVO.getConfigExecute(), TemplConfig , "ConfigExecute"  );
        appendStringBuffer( messageTemplateVO.getConfigPostExec(), TemplConfig , "ConfigPostExec"  );
        appendStringBuffer( messageTemplateVO.getHeaderXSLT(), TemplConfig , "HeaderXSLT"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeInXSLT(), TemplConfig , "EnvelopeInXSLT"  );


        appendStringBuffer( messageTemplateVO.getEnvelopeXSLTExt(), TemplConfig , "EnvelopeXSLTExt"  );
        appendStringBuffer( messageTemplateVO.getEnvelopeNS(), TemplConfig , "EnvelopeNS"  );
        appendStringBuffer( messageTemplateVO.getMessageAnswAck(), TemplConfig , "MessageAnswAck"  );
        appendStringBuffer( messageTemplateVO.getMessageAnswMsgXSLT(), TemplConfig , "MessageAnswMsgXSLT"  );

        appendStringBuffer( messageTemplateVO.getMessageAnswerXSD(), TemplConfig , "MessageAnswerXSD"  );

        appendStringBuffer( messageTemplateVO.getAckAnswXSLT(), TemplConfig , "AckAnswXSLT"  );

        appendStringBuffer( messageTemplateVO.getErrTransXSLT(), TemplConfig , "ErrTransXSLT"  );
        appendStringBuffer( messageTemplateVO.getErrTransXSD(), TemplConfig , "ErrTransXSD"  );

        TemplConfig.append( Conf_Text_End );
        return TemplConfig.toString();
    }

    static public int getMessageTemplateVO( String MessageDirection, String sInterface_Id, String sOperation_Id, String MsgType_Direction  ) {
        // MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        // Зачитывем
        int numTemplatefound =
        InitTemplates_InitTypes.SelectMsgTemplates( MessageDirection,
                Integer.parseInt(sInterface_Id),
                Integer.parseInt(sOperation_Id),
                MsgType_Direction );
        return  numTemplatefound;
    }


    static public int getMessageTypeVO( String sInterface_Id, String sOperation_Id  ) {
        // MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        // Зачитывем
        int numMessageTypeFound =
                InitTemplates_InitTypes.SelectMsgTypes(
                        Integer.parseInt(sInterface_Id),
                        Integer.parseInt(sOperation_Id) );
        return  numMessageTypeFound;
    }

    static public int newMessageTemplateVO(MessageTypeVO messageTypeVO, String MessageDirection ) {
        // MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        // Зачитываем
        int numTemplatefound = 0;
        MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        String TemplateSystemName;
        if ( MessageDirection.equals("-X-") )
            TemplateSystemName = "";
        else
            TemplateSystemName = " для системы " + MessageDirection;
        Integer Source_Id = 0;
        Integer Destin_Id = 0;
        String Src_SubCod = null;
        String Dst_SubCod = null;


        int systemFound = InitTemplates_InitTypes.SelectMsgDirections( MessageDirection );
        if ( systemFound >0) {
            if ( messageTypeVO.getMsg_Direction().equalsIgnoreCase("IN") )
            { // // для входящих получатель  всегда Гермес destin_id = 5
                Source_Id = MessageDirections.AllMessageDirections.get(0).getMsgDirection_Id();
                Destin_Id = 5;
                Src_SubCod = MessageDirections.AllMessageDirections.get(0).getSubsys_Cod();
                Dst_SubCod = null;
            }
            else // для исходящих источник всегда Гермес
            {
                Source_Id = 5;
                Destin_Id = MessageDirections.AllMessageDirections.get(0).getMsgDirection_Id();
                Src_SubCod = null;
                Dst_SubCod = MessageDirections.AllMessageDirections.get(0).getSubsys_Cod();
            }

        }

        messageTemplateVO.setMessageTemplateVO(
                0, //("template_id"),
                messageTypeVO.getInterface_Id(), //rs.getInt("Interface_Id"),
                messageTypeVO.getOperation_Id(), //rs.getInt("Operation_Id"),
                Source_Id, // 0, //rs.getInt("Source_Id"),
                Src_SubCod, // null,  //rs.getString("Src_SubCod"),
                Destin_Id, // 0, //rs.getInt("Destin_Id"),
                Dst_SubCod, // null, // rs.getString("Dst_SubCod"),
                messageTypeVO.getMsg_Type(), // rs.getString("Msg_Type"),
                messageTypeVO.getMsg_Type_own(),  //rs.getString("Msg_Type_own"),
                messageTypeVO.getMsg_TypeDesc()+ TemplateSystemName, //rs.getString("Template_name"),
                messageTypeVO.getMsg_Direction(), //rs.getString("Template_Dir"),
                "INFO",
                Conf_Text_Begin + Conf_Text_End, //rs.getString("Conf_Text"),
                "MasterCopy", // rs.getString("LastMaker"),
                messageTypeVO.getLast_Update_Dt().toString() //rs.getString("LastDate")
        );
        numTemplatefound = ConfigMsgTemplates.performConfig(messageTemplateVO , null );
        MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);
        System.out.println(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() +" parseConfigResult=" + numTemplatefound);

        MessageTemplate.RowNum += 1;
        return  numTemplatefound;
    }
    /*
;
     */
/*
    static private void Perform() {
        Statement stmt_SELECT = null;
        Statement stmt_COMMIT = null;
        ResultSet rs_SELECT = null;
        ResultSetMetaData col_MetaData;
        Integer col_count;
        int totalRows;
        Boolean rs_isFirst;
        try {


            String SQL_SELECT = "SELECT TOP " +
                    " branch_name, fio, email, num_tab, hire_date, fire_date, employeeid, date_insert, date_modify FROM Persons_fired_VV";

            System.out.println("SQL_SELECT: " + SQL_SELECT);


            stmt_COMMIT = jAnyDbDeploy.Oracle_Connection.createStatement();

            String sql = "" ; ///""truncate table HERMES_AUTH.X_Persons_fired_VV drop all storage";
            stmt_COMMIT.executeUpdate(sql);

            sql = "COMMIT";

            stmt_SELECT = jAnyDbDeploy.Oracle_Connection.createStatement();
            rs_SELECT = stmt_SELECT.executeQuery(SQL_SELECT);
            rs_isFirst = true;

            totalRows = 0;
            // Iterate through the data in the result set and display it.
            while (rs_SELECT.next()) {

                if (rs_isFirst) {
                    Integer i;
                    col_MetaData = rs_SELECT.getMetaData();
                    col_count = col_MetaData.getColumnCount();
                    System.out.print("col_count( " + col_count + " ) : ");
                    rs_isFirst = false;
                    for (i = 1; i <= col_count; i++) {
                        System.out.print(col_MetaData.getColumnName(i) + "|");
                    }
                    System.out.println("  ");
                }
                if ( is_verbose.equalsIgnoreCase("Y" ) ) {
                    System.out.println(
                            rs_SELECT.getString(1) + "|" +
                                    rs_SELECT.getString(2) + "| " +
                                    rs_SELECT.getString(3) + "|" +
                                    rs_SELECT.getString(4) + "|" +
                                    rs_SELECT.getString(5) + "|" +
                                    rs_SELECT.getString(6) + "|" +
                                    rs_SELECT.getString(7) + "|" +
                                    rs_SELECT.getString(8) + "|" +
                                    rs_SELECT.getString(9) + "|"  );
                }

                stmt_INSERT.setString(1, rs_SELECT.getString(1));
                stmt_INSERT.setString(2, rs_SELECT.getString(2));
                stmt_INSERT.setString(3, rs_SELECT.getString(3));
                stmt_INSERT.setString(4, rs_SELECT.getString(4));
                stmt_INSERT.setDate(5, rs_SELECT.getDate(5));
                stmt_INSERT.setDate(6, rs_SELECT.getDate(6));
                stmt_INSERT.setString(7, rs_SELECT.getString(7));
                //stmt_INSERT.setString(8, rs_SELECT.getString(8));
                // stmt_INSERT.setString(9, rs_SELECT.getString(9));
                int affectedRows = stmt_INSERT.executeUpdate();
                totalRows = totalRows + affectedRows;
                if ( (totalRows % 100) == 0  ) {
                    System.out.println(totalRows + " row(s) affected !");
                    stmt_COMMIT.executeUpdate(sql);
                }
                //System.out.println(affectedRows + " row(s) affected !!");

            }
            System.out.println(totalRows + " row(s) affected !!");
            stmt_COMMIT.executeUpdate(sql);

            System.out.println("call Hermes_Auth.x_Persons_Perform.Lock_Fired_Persons()" );
            // register OUT parameter
            callableStatement.registerOutParameter(1, Types.INTEGER);

            // Step 2.C: Executing CallableStatement
            callableStatement.execute();

            // get count and print in console
            int count = callableStatement.getInt(1);
            System.out.println(
                    "Blocked count in HERMES_AUTH.AUTH_USER database is = " + count);
            Oracle_Connection.commit();

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (rs_SELECT != null) try {
                rs_SELECT.close();
            } catch (Exception e) { e.printStackTrace();
            }
            if (stmt_SELECT != null) try {
                stmt_SELECT.close();
            } catch (Exception e) { e.printStackTrace();
            }
            if (Oracle_Connection != null) try {
                Oracle_Connection.close();
            } catch (Exception e) { e.printStackTrace();
            }
        }
    }
*/
}
