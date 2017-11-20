#ifndef _HFS_BASEINTERFACE_H_
#define _HFS_BASEINTERFACE_H_
#include "DataDef.h"
#include "smartptr.h"

class CBaseInterface:public CRefCounter
{
public:
	CBaseInterface(void);
	~CBaseInterface(void);

	virtual hfsint		ReadRecord(vector<TASKDATA*>& taskdata) = 0;
	virtual hfsint		ReadRecordLoop(vector<TASKDATA*>& taskdata) = 0;
	virtual hfsbool		SetStatus(hfsint64 id,hfsint status) = 0;
	virtual hfsbool		WriteResult(hfsint64 id,pchfschar pBuffer,hfsint64 recCount = 0,hfsint64 citeCount=0,hfsint64 cited_count=0) = 0;
	virtual hfsint		DoWriteRecord(DOWN_STATE* taskdata) = 0;
	virtual hfsbool		LoadZJCLS() = 0;
	virtual hfsbool		LoadAuthor() = 0;
	virtual hfsbool		LoadHosp() = 0;
	virtual hfsbool		LoadUniv() = 0;
	virtual void		SetSystemInfo(SYSTEMINFO *systemInfo) = 0;
};

typedef Ptr<CBaseInterface> CBasePtr;
#endif