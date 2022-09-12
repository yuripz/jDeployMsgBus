package net.plumbing.msgbus.common;

public class XMLchars {
    public static final String Space=" ";
    public static final String Quote="\"";
    public static final String Equal="=";
    public static final String OpenTag="<";
    public static final String CloseTag=">";
    public static final String EndTag="/";
    public static final String XMLns="xmlns:";
    public static final String CDATAopen="<![CDATA[";
    public static final String CDATAclose="]]>";
    //public static final String Envelope_Begin="<env:Envelope xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\">";
    public static final String Envelope_Begin="<env:Envelope xmlns:env=\"http://schemas.xmlsoap.org/soap/envelope/\" "
    + "xmlns:urn=\"urn:DefaultNamespace\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" xmlns:soapenc=\"http://schemas.xmlsoap.org/soap/encoding/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\""
    + ">";
    public static final String Envelope_End="</env:Envelope>";
    public static final String Empty_Header="<env:Header/>";
    public static final String Header_Begin="<env:Header>";
    public static final String Header_End="</env:Header>";
    public static final String Body_Begin="<env:Body>";
    public static final String Body_End="</env:Body>";
    public static final String Fault_Begin="<env:Fault><faultcode>env:Client</faultcode><faultstring>";
    public static final String Fault_End="</faultstring></env:Fault>";
    final public static String TagContext     = "Context";
    final public static String TagEventInit   = "EventInitiator";
    final public static String TagEventKey    = "EventKey";
    final public static String TagEventSrc    = "Source";
    final public static String TagEventDst    = "Destination";
    final public static String TagEventOpId   = "BusOperationId";


    final public static String TagEntryRec    = "Request";
    final public static String TagEntryInit   = "init";
    final public static String TagEntryKey    = "key";
    final public static String TagEntrySrc    = "src";
    final public static String TagEntryDst    = "dst";
    final public static String TagEntryOpId   = "opid";

    final public static String HermesMsgDirection_Cod   = "HRMS";
    final public static String xml_xml ="<?xml version=\"1.0\" encoding=\"UTF-8\"?>";

    public static final String TagMsgHeaderEmpty  = "Header_is_empty";

    public static final String Body="Body";
    public static final String NameRootTagContentJsonResponse    = "MsgData";
    public static final String Envelope="Envelope";
    public static final String TagConfirmation="Confirmation";
    public static final String TagDetailList="DetailList";
    public static final String TagNext ="Next";

    public static final String NameTagResult       = "Result";
    public static final String NameTagResultCod    = "Cod";
    public static final String NameTagResultText   = "Text";

/*    public static final String NameTagFault        = "Fault";
    public static final String NameTagFaultNs      = "FaultNS";
    public static final String NameTagFaultCode    = "FaultCode";
    public static final String NameTagFaultResult  = "ResultCode";
    public static final String NameTagFaultTxt     = "Message";*/

    final public static String DirectWAITOUT = "WAITOUT";
    final public static String DirectOUT     = "OUT";
    final public static String DirectERROUT  = "ERROUT";
    final public static String DirectSEND    = "SEND";
    final public static String DirectEXEOUT  = "EXEOUT";
    final public static String DirectRESOUT  = "RESOUT";
    final public static String DirectPOSTOUT = "POSTOUT";
    final public static String DirectATTNOUT = "ATTOUT";
    final public static String DirectDELOUT  = "DELOUT";


    public static final String Envelope_noNS_Begin="<Envelope>";
    public static final String Envelope_noNS_End="</Envelope>";
    public static final String Header_noNS_Begin="<Header>";
    public static final String Header_noNS_End="</Header>";
    public static final String Body_noNS_Begin="<Body>";
    public static final String Body_noNS_End="</Body>";

    public static final String EmptyXSLT_Result ="<?xml version=\"1.0\" encoding=\"utf-8\"?><nan></nan>";
      public static final String nanXSLT_Result ="<nan></nan>";

}
