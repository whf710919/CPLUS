#ifndef HFS_SERVICEBASE_H_
#define HFS_SERVICEBASE_H_

#include "DataDef.h"
#include "smartptr.h"
#include "Error.h"
#include "GLock.h"
#include "Utility.h"
#include "Markup.h"
#include "CTaskFactory.h"

class CServiceBase;
typedef Ptr<CServiceBase> CServiceBasePtr;

#ifdef _WINDOWS_
class CServiceBase:public CRefCounter
#else
class CServiceBase:public CRefCounter
#endif
{
public:
	CServiceBase(void);
	~CServiceBase(void);
	virtual hfsbool		InitInstance();
	virtual	hfsint		ReadRecord(){return 0;}
	virtual	hfsint		ReadRecordLoop(){return 0;}
	virtual	hfsint		SetMonth(int i){return 0;}
	virtual	hfsbool		DoProcessThread(){return true;}
	virtual	hfsbool		DownLoadFile(){return 0;}

	static CTaskListPtr		Task();
	static CServiceBasePtr	Invoker();	
protected:

	hfsint			ParserServiceType(CMarkup &xParser);
	string			ParserServiceName(CMarkup &xParser);
	hfsint			ParserParent(CMarkup &xParser);
	string			ParserHost(CMarkup &xParser);
	string			ParserWorkDisk(CMarkup &xParser);
	hfsint			ParserMaster(CMarkup &xParser);
	hfsint			ParserStatus(CMarkup &xParser);
	hfsint			ParserExportValue(CMarkup &xParser);
	hfsint			ParserTotalNode(CMarkup &xParser);

	hfsbool			PutServiceType(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutServiceName(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutParent(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutserHost(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutWorkDisk(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutMaster(CMarkup &xParser,HFS_NODE_INFO *node);
	hfsbool			PutStatus(CMarkup &xParser,HFS_NODE_INFO *node);


	G_Lock			m_Lock;
};


#endif