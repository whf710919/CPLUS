#include "StdAfx.h"
#include "ServiceBase.h"
#include "WinFile.h"
#include "SmartBuffer.h"
#include "CTaskFactory.h"

CServiceBasePtr	m_this(0);
#define ADMINUSER	("admin")
#define ADMINTOKEN	("admin")

static CTaskListPtr		m_Tasks(0);

CServiceBase::CServiceBase(void)
{
	m_this = this;
}


CServiceBase::~CServiceBase(void)
{
}


hfsbool CServiceBase::InitInstance()
{
	m_Tasks = new CTaskList();

	//CUtility::Logger()->setmsgsize(CUtility::Config()->GetMsgCacheSize());

	//CUtility::Logger()->setwriter(WriteMsgQueue);
	//CUtility::Logger()->setlogger(WriteTraceQueue);

	return true;
}


hfsint CServiceBase::ParserServiceType(CMarkup &xParser)
{
	string t;
	if(xParser.FindElem("ServiceType")){
		t = xParser.GetData();
		if(!stricmp(t.c_str(),"REF")){
			return REF_SERVICE;
		}
		if(!stricmp(t.c_str(),"REC")){
			return REC_SERVICE;
		}
		if( !stricmp(t.c_str(),"CONFIG")){
			return CONFIG_SERVICE;
		}
		if( !stricmp(t.c_str(),"MONITOR")){
			return MONITOR_SERVICE;
		}
		if( !stricmp(t.c_str(),"LOG")){
			return LOG_SERVICE;
		}
		if( !stricmp(t.c_str(),"MSG")){
			return MSG_SERVICE;
		}
		else{
			return -1;
		}
	}else{
		return -1;
	}
}


hfsbool CServiceBase::PutServiceType(CMarkup &xParser,HFS_NODE_INFO *node)
{
	string t;
	if(node->NodeType == REF_SERVICE){
		if(!xParser.AddChildElem("ServiceType","REF")){
			return false;
		}
	}
	else if(node->NodeType == CONFIG_SERVICE){
		if(!xParser.AddChildElem("ServiceType","CONFIG")){
			return false;
		}
	}
	else if(node->NodeType == LOG_SERVICE){
		if(!xParser.AddChildElem("ServiceType","LOG")){
			return false;
		}
	}
	else if(node->NodeType == MONITOR_SERVICE){
		if(!xParser.AddChildElem("ServiceType","MONITOR")){
			return false;
		}
	}
	else if(node->NodeType == MSG_SERVICE){
		if(!xParser.AddChildElem("ServiceType","MSG")){
			return false;
		}
	}
	else{
		return false;
	}
	return true;
}

hfsbool CServiceBase::PutServiceName(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("ServiceName",node->NodeName)){
		return false;
	}
	return true;
}

string	CServiceBase::ParserServiceName(CMarkup &xParser)
{
	if(xParser.FindElem("ServiceName")){
		return xParser.GetData();
	}
	return "";
}

hfsbool CServiceBase::PutParent(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("ParentService",node->ParentID)){
		return false;
	}
	return true;
}

hfsint CServiceBase::ParserParent(CMarkup &xParser)
{
	if(xParser.FindElem("ParentService")){
		return atoi(xParser.GetData().c_str());
	}
	return -1;
}

hfsbool CServiceBase::PutserHost(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("IP",node->IP)){
		return false;
	}
	return true;
}

string CServiceBase::ParserHost(CMarkup &xParser)
{
	if(xParser.FindElem("IP")){
		return xParser.GetData();
	}
	return "";
}

hfsbool CServiceBase::PutWorkDisk(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("WorkDisk",node->WorkDisk)){
		return false;
	}
	return true;
}

string	CServiceBase::ParserWorkDisk(CMarkup &xParser)
{
	if(xParser.FindElem("WorkDisk")){
		return xParser.GetData();
	}
	return "";
}

hfsbool CServiceBase::PutMaster(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("Master",node->MasterID)){
		return false;
	}
	return true;
}

hfsint CServiceBase::ParserMaster(CMarkup &xParser)
{
	if(xParser.FindElem("Master")){
		return atoi(xParser.GetData().c_str());
	}
	return -1;
}

hfsbool CServiceBase::PutStatus(CMarkup &xParser,HFS_NODE_INFO *node)
{
	if(!xParser.AddChildElem("Status",node->Status.NodeStatus)){
		return false;
	}
	return true;
}

hfsint CServiceBase::ParserStatus(CMarkup &xParser)
{
	if(xParser.FindElem("Status")){
		return atoi(xParser.GetData().c_str());
	}
	return -1;
}

hfsint CServiceBase::ParserExportValue(CMarkup &xParser)
{
	if(xParser.FindElem("ExportValue")){
		return atoi(xParser.GetData().c_str());
	}
	return -1;
}

hfsint CServiceBase::ParserTotalNode(CMarkup &xParser)
{
	if(xParser.FindElem("TotalNode")){
		return atoi(xParser.GetData().c_str());
	}
	return -1;
}


CTaskListPtr CServiceBase::Task()
{
	return m_Tasks;
}


CServiceBasePtr CServiceBase::Invoker()
{
	return m_this;
}


