package net.plumbing.msgbus.init;

// import org.slf4j.Logger;
import net.plumbing.msgbus.common.sStackTracе;
import net.plumbing.msgbus.jAnyDbDeploy;
import net.plumbing.msgbus.model.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class InitTemplates_InitTypes {

  //  private static final String HrmsSchema="orm";
    public static  int SelectMsgDirections(
            String HrmsSchema,
            String MsgDirection_Cod )  {
        PreparedStatement stmtMsgDirection = null;
        ResultSet rs = null;


        MessageDirections.AllMessageDirections.clear();

        if ( jAnyDbDeploy.Oracle_Connection != null )
            try {
                // stmtMsgDirectionReRead = DataAccess.Hermes_Connection.prepareStatement(SQLMsgDirectionReRead );

                stmtMsgDirection = jAnyDbDeploy.Oracle_Connection.prepareStatement("SELECT f.msgdirection_id," +
                        " f.msgdirection_cod," +
                        " f.msgdirection_desc," +
                        " f.app_server," +
                        " f.wsdl_name," +
                        " f.msgdir_own," +
                        " f.operator_id," +
                        " f.type_connect," +
                        " f.db_name," +
                        " f.db_user," +
                        " f.db_pswd," +
                        " f.subsys_cod," +
                        " f.base_thread_id," +
                        " f.num_thread," +
                        " Coalesce(f.Short_retry_count,0) Short_retry_count, " +
                        " Coalesce(f.Short_retry_interval,0) Short_retry_interval," +
                        " Coalesce(f.Long_retry_count,0) Long_retry_count, " +
                        " Coalesce(f.Long_retry_interval,0) Long_retry_interval" +
                         // " ,f.Source_Id, f.Destin_Id, f.Src_SubCod, f.Dst_SubCod" +
                        " FROM "+ HrmsSchema + ".Message_Directions F where f.msgdirection_cod = '" + MsgDirection_Cod + "'" +
                        " order by f.msgdirection_iD,  f.subsys_cod,  f.msgdirection_cod");

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else
        {
            return -3;
        }

        try {
            rs = stmtMsgDirection.executeQuery();
            while (rs.next()) {
                MessageDirectionsVO messageDirectionsVO = new MessageDirectionsVO();
                messageDirectionsVO.setMessageDirectionsVO(
                        rs.getInt("msgdirection_id"),
                        rs.getString("msgdirection_cod"),
                        rs.getString("Msgdirection_Desc"),
                        rs.getString("app_server"),
                        rs.getString("wsdl_name"),
                        rs.getString("msgdir_own"),
                        rs.getString("operator_id"),
                        rs.getInt("type_connect"),
                        rs.getString("db_name"),
                        rs.getString("db_user"),
                        rs.getString("db_pswd"),
                        rs.getString("subsys_cod"),
                        rs.getInt("base_thread_id"),
                        rs.getInt("num_thread"),
                        rs.getInt("short_retry_count"),
                        rs.getInt("short_retry_interval"),
                        rs.getInt("long_retry_count"),
                        rs.getInt("long_retry_interval")
                        // " f.Source_Id, f.Destin_Id, f.Src_SubCod, f.Dst_SubCod"
                        // rs.getInt("Source_Id"), rs.getInt("Destin_Id"), rs.getString("Src_SubCod"), rs.getString("Dst_SubCod")
                );
                // messageDirectionsVO.LogMessageDirections( log );


                MessageDirections.AllMessageDirections.put( MessageDirections.RowNum, messageDirectionsVO );
                System.out.println( "RowNum[" + MessageDirections.RowNum + "] =" + messageDirectionsVO.LogMessageDirections() );

                MessageDirections.RowNum += 1;
                System.out.println(MessageDirections.AllMessageDirections.get(0).LogMessageDirections() );
                // log.info(" MessageDirections[" +   MessageDirections.AllMessageDirections.size() + "]: longRetryInterval=" + messageDirectionsVO.getLong_retry_interval() + ", "+ messageDirectionsVO.getMsgDirection_Desc() );

            }
            rs.close();
            //  stmtMsgDirectionReRead.close();
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageDirections.RowNum;
    }


    public static int SelectMsgTemplates(String HrmsSchema, String MessageDirections, Integer Interface_Id, Integer Operation_Id, String TypeMsg_Direction) {
        int parseResult;
        PreparedStatement stmtMsgTemplate;
        ResultSet rs;

        //MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
        MessageTemplate.AllMessageTemplate.clear();
        MessageTemplate.RowNum = 0;
        String where_x_templates_look_2_MessageDirections ;
        if ( TypeMsg_Direction.equalsIgnoreCase("OUT") )
            where_x_templates_look_2_MessageDirections = " " + HrmsSchema + ".look_2_MessageDirections(t.destin_id, t.dst_subcod) ";
        else
            where_x_templates_look_2_MessageDirections = " " + HrmsSchema + " .look_2_MessageDirections(t.source_id, t.src_subcod) ";
        String Select_Templates = "select t.template_id, " +
                "t.interface_id, " +
                "t.operation_id, " +
                "t.msg_type, " +
                "t.msg_type_own, " +
                "t.template_name, " +
                "t.template_dir, " +
                "t.source_id, " +
                "t.destin_id, " +
                "t.conf_text, " +
                "t.src_subcod, " +
                "t.dst_subcod, " +
                "t.lastmaker, " +
                "to_char(t.lastdate,'YYYY.MM.DD HH24:MI:SS') LastDate " +
                "from " + HrmsSchema + ".MESSAGE_TemplateS t " +
                "where (1=1) "
              //  + " and " + where_x_templates_look_2_MessageDirections + " = '" + MessageDirections + "'"
                + " and t.Interface_Id = " + Interface_Id.toString()
                + " and t.Operation_Id = " + Operation_Id.toString();
        System.out.println(Select_Templates);

        if (jAnyDbDeploy.Oracle_Connection != null)
            try {

                stmtMsgTemplate = jAnyDbDeploy.Oracle_Connection.prepareStatement(Select_Templates
                );

            } catch (Exception e) {
                e.printStackTrace();
                return -2;
            }
        else {
            System.err.println("jAnyDbDeploy.Oracle_Connection is null!");
            return -3;
        }

        try {
            rs = stmtMsgTemplate.executeQuery();
            while (rs.next()) {
                MessageTemplateVO messageTemplateVO = new MessageTemplateVO();
                messageTemplateVO.setMessageTemplateVO(
                        rs.getInt("template_id"),
                        rs.getInt("Interface_Id"),
                        rs.getInt("Operation_Id"),
                        rs.getInt("Source_Id"),
                        rs.getString("Src_SubCod"),
                        rs.getInt("Destin_Id"),
                        rs.getString("Dst_SubCod"),
                        rs.getString("Msg_Type"),
                        rs.getString("Msg_Type_own"),
                        rs.getString("Template_name"),
                        rs.getString("Template_Dir"),
                        "INFO",
                        rs.getString("Conf_Text"),
                        rs.getString("LastMaker"),
                        rs.getString("LastDate")
                );
                //messageTypeVO.LogMessageDirections( log );

                System.out.println(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size());

                parseResult = ConfigMsgTemplates.performConfig(messageTemplateVO, null);
                MessageTemplate.AllMessageTemplate.put(MessageTemplate.RowNum, messageTemplateVO);

                System.out.println(" AllMessageTemplate.size :" + MessageTemplate.AllMessageTemplate.size() + " MessageRowNum =" + MessageTemplate.RowNum +
                        " Template_name:" + MessageTemplate.AllMessageTemplate.get(MessageTemplate.RowNum).getTemplate_name() + " parseConfigResult=" + parseResult);

                MessageTemplate.RowNum += 1;

            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageTemplate.RowNum;
    }

    public static int SelectMsgTypes(String HrmsSchema, Integer Interface_Id, Integer Operation_Id) {
        PreparedStatement stmtMsgType = null;
        ResultSet rs = null;
        String MsgTypeSelect= "select t.interface_id,\n" +
                "t.operation_id,\n" +
                "t.msg_type,\n" +
                "t.msg_type_own,\n" +
                "t.msg_typedesc,\n" +
                "t.msg_direction,\n" +
                "t.msg_handler,\n" +
                "t.url_soap_send,\n" +
                "t.url_soap_ack,\n" +
                "t.max_retry_count,\n" +
                "t.max_retry_time, last_update_dt \n" +
                "from " + HrmsSchema + ".MESSAGE_typeS t\n" +
                "where (1=1) "
                + " and t.Interface_Id = " + Interface_Id.toString()
                + " and t.Operation_Id = " + Operation_Id.toString();
        MessageType.AllMessageType.clear();
        MessageType.RowNum = 0;

        if (jAnyDbDeploy.Oracle_Connection != null)
            try {
                stmtMsgType = jAnyDbDeploy.Oracle_Connection.prepareStatement( MsgTypeSelect
                );

            } catch (Exception e) {
                System.err.println( "SelectMsgTypes: " + MsgTypeSelect + " fault ");
                e.printStackTrace();
                return -2;
            }
        else {
            return -3;
        }
        System.out.println( "SelectMsgTypes: try [" + MsgTypeSelect + " ] ");
        try {
            rs = stmtMsgType.executeQuery();
            while (rs.next()) {
                MessageTypeVO messageTypeVO = new MessageTypeVO();
                messageTypeVO.setMessageTypeVO(
                        rs.getInt("interface_id"),
                        rs.getInt("operation_id"),
                        rs.getString("msg_type"),
                        rs.getString("msg_type_own"),
                        rs.getString("msg_typedesc"),
                        rs.getString("msg_direction"),
                        rs.getInt("msg_handler"),
                        rs.getString("url_soap_send"),
                        rs.getString("url_soap_ack"),
                        rs.getInt("max_retry_count"),
                        rs.getInt("max_retry_time"),
                        rs.getTimestamp("Last_Update_Dt")
                );
                //messageTypeVO.LogMessageDirections( log );
                // log.info(" messageTypeVO :", messageTypeVO );
                // log.info(" AllMessageType.size :" +   MessageType.AllMessageType.size() );

                MessageType.AllMessageType.put(MessageType.RowNum, messageTypeVO);
                MessageType.RowNum += 1;

                System.out.println(" Types.size=" + MessageType.AllMessageType.size() + ", MessageRowNum[" + MessageType.RowNum + "] :" + messageTypeVO.getMsg_Type());
            }
        } catch (Exception e) {
            e.printStackTrace();
            return -2;
        }
        return MessageType.RowNum;
    }

    private static PreparedStatement stmt_INSERT_MESSAGE_TemplateS=null;
    private static PreparedStatement stmt_maxTemplate_Id=null;
    private static PreparedStatement stmt_update_MESSAGE_Template=null;
    private static String INSERT_MESSAGE_Template;

    private static PreparedStatement make_INSERT_MessageTemplate( String HrmsSchema) {
        PreparedStatement StmtMsg_Queue;
        INSERT_MESSAGE_Template = "insert into " + HrmsSchema + ".Message_Templates( " +
                "Interface_Id,\n" +
                "Operation_Id,\n" +
                "Msg_Type,\n" +
                "Msg_Type_Own,\n" +
                "Template_Name,\n" +
                "Template_Dir,\n" +
                "Source_Id,\n" +
                "Destin_Id,\n" +
                "Src_Subcod,\n" +
                "Dst_Subcod,\n" +
                "Lastmaker,\n" +
                "Lastdate, Template_Id ) \n select  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,  current_timestamp, ? from dual";



        try {
            StmtMsg_Queue = jAnyDbDeploy.Oracle_Connection.prepareStatement(INSERT_MESSAGE_Template);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            return ((PreparedStatement) null);
        }
        stmt_INSERT_MESSAGE_TemplateS = StmtMsg_Queue;
        try {
            stmt_maxTemplate_Id = jAnyDbDeploy.Oracle_Connection.prepareStatement("select max(Template_Id)+1 from " + HrmsSchema + ".Message_Templates");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            return ((PreparedStatement) null);
        }
        return StmtMsg_Queue;
    }
    private static String update_MESSAGE_Template;
    private static PreparedStatement make_update_MESSAGE_Template( String HrmsSchema ) {
        PreparedStatement StmtMsg_Queue;
         update_MESSAGE_Template = "update " + HrmsSchema +  ".Message_Templates set conf_text=?, Lastmaker=?, Lastdate=current_timestamp where Template_Id =?";
        try {

            StmtMsg_Queue = jAnyDbDeploy.Oracle_Connection.prepareStatement(update_MESSAGE_Template);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
            return ((PreparedStatement) null);
        }
        stmt_update_MESSAGE_Template = StmtMsg_Queue;
        return StmtMsg_Queue;
    }

    public static int doUpdate_MESSAGE_Template( String HrmsSchema, Integer maxTemplate_Id, String Conf_Text ) {
        make_update_MESSAGE_Template( HrmsSchema );
        if (stmt_update_MESSAGE_Template != null) {
            try {
                stmt_update_MESSAGE_Template.setString(1, Conf_Text );
                String LastMaker = "j." + System.getProperty("user.name") + "." + System.getenv("COMPUTERNAME");
                stmt_update_MESSAGE_Template.setString(2, LastMaker );
                stmt_update_MESSAGE_Template.setInt(3, maxTemplate_Id);
                stmt_update_MESSAGE_Template.executeUpdate();
                System.out.println(  ">" + update_MESSAGE_Template + ":Template_Id=[" + maxTemplate_Id + "] done");

                jAnyDbDeploy.Oracle_Connection.commit();
            } catch (SQLException e) {
                System.err.println(">" + update_MESSAGE_Template + ":Template_Id=[" + maxTemplate_Id + "] :" + sStackTracе.strInterruptedException(e));
                e.printStackTrace();
                try {
                    jAnyDbDeploy.Oracle_Connection.rollback();
                } catch (SQLException exp) {
                    System.err.println("Hermes_Connection.rollback()fault: " + exp.getMessage());
                }
                return -11;
            }
        }
        return 0;
    }
    public static int doINSERT_MessageTemplate( String HrmsSchema, MessageTemplateVO messageTemplateVO) {
        make_INSERT_MessageTemplate( HrmsSchema );
        Integer maxTemplate_Id=null;
        if ( stmt_INSERT_MESSAGE_TemplateS !=null) {
        try {
               ResultSet rs = stmt_maxTemplate_Id.executeQuery();
                while (rs.next()) {
                    maxTemplate_Id = rs.getInt(1);
                }
                rs.close();
                stmt_maxTemplate_Id.close();

            stmt_INSERT_MESSAGE_TemplateS.setInt(1, messageTemplateVO.getInterface_Id()); //rs.getInt("Interface_Id"),
            stmt_INSERT_MESSAGE_TemplateS.setInt(2, messageTemplateVO.getOperation_Id()); // rs.getInt("Operation_Id"),
            stmt_INSERT_MESSAGE_TemplateS.setString(3, messageTemplateVO.getMsg_Type());  // rs.getString("Msg_Type"),
            stmt_INSERT_MESSAGE_TemplateS.setString(4, messageTemplateVO.getMsg_Type_own()); // rs.getString("Msg_Type_own"),
            stmt_INSERT_MESSAGE_TemplateS.setString(5, messageTemplateVO.getTemplate_name()); //  rs.getString("Template_name"),
            stmt_INSERT_MESSAGE_TemplateS.setString(6, messageTemplateVO.getTemplate_Dir()); //  rs.getString("Template_Dir"),

            stmt_INSERT_MESSAGE_TemplateS.setInt(7, messageTemplateVO.getSource_Id()); //        rs.getInt("Source_Id"),
            stmt_INSERT_MESSAGE_TemplateS.setInt(8, messageTemplateVO.getDestin_Id()); //   rs.getInt("Destin_Id"),

            stmt_INSERT_MESSAGE_TemplateS.setString(9, messageTemplateVO.getSrc_SubCod()); //  rs.getString("Src_SubCod"),
            stmt_INSERT_MESSAGE_TemplateS.setString(10, messageTemplateVO.getDst_SubCod()); //  rs.getString("Dst_SubCod"),
            String LastMaker = "j." + System.getProperty("user.name") + "." + System.getenv("COMPUTERNAME");

            stmt_INSERT_MESSAGE_TemplateS.setString(11, // messageTemplateVO.getLastMaker()
                                                     LastMaker.length() > 50 ? LastMaker.substring(0, 50) : LastMaker)
            ; // rs.getString("LastMaker"),
            System.getProperty("user.name");
            System.getenv("COMPUTERNAME");
            stmt_INSERT_MESSAGE_TemplateS.setInt(12, maxTemplate_Id );

            stmt_INSERT_MESSAGE_TemplateS.executeUpdate();
            System.out.println(  ">" + INSERT_MESSAGE_Template + ":Template_name=[" + messageTemplateVO.getTemplate_name() + "] done");

            jAnyDbDeploy.Oracle_Connection.commit();
        } catch (SQLException e) {
            System.err.println(">" + INSERT_MESSAGE_Template + ":Template_name=[" + messageTemplateVO.getTemplate_name() + "] :" + sStackTracе.strInterruptedException(e));
            e.printStackTrace();
            try {
                jAnyDbDeploy.Oracle_Connection.rollback();
            } catch (SQLException exp) {
                System.err.println("Hermes_Connection.rollback()fault: " + exp.getMessage());
            }
            return -11;
        }

        }
        return maxTemplate_Id;
    }

}