#ifndef HFS_KFSBASE_H_
#define HFS_KFSBASE_H_
#include "DataDef.h"
#include "BaseInterface.h"
#include "smartptr.h"
#include "KBase.h"
#include "GLock.h"
#include "dob.h"
#include "ServerTempFile.h"
#include "HandleBase.h"
#include "Utility.h"

#define MAX_CONN_COUNT (1024)

class CKFSHanle:public CHandleBase
{
private:
	CKFSHanle():CHandleBase(){}
	CKBConnPtr	m_hConn;
	hfsuint64	ptr;
	time_t		c_time;
	hfststring	m_mode;
	CFileDriverPtr	f;
	
public:
	HFS_FILE_INFO m_ifo;

	CKFSHanle(CKBConnPtr hConn,pchfschar mode,CFileDriverPtr p)
	{
		m_hConn = hConn;
		m_mode	= mode;
		f = p;
		time(&c_time);
		ptr = 0;

		memset(&m_ifo,0x00,sizeof(HFS_FILE_INFO));

	}

	~CKFSHanle()
	{
		f = (CFileDriverPtr)0;
	}

	inline CKBConnPtr GetConn()
	{
		time(&c_time);
		return m_hConn;
	}
	inline hfsuint64 GetPtr()
	{
		time(&c_time);
		return ptr;
	}
	inline hfsuint64 SetPtr(hfsuint64 p)
	{
		time(&c_time);
		ptr=p;
		return ptr;
	}
	inline hfsbool IsTimeOut()
	{
		time_t now;
		time(&now);

		if( now - c_time >= 
			(HANDLE_TIMEOUT_SEC)){
			return true;
		}

		return false;
	}

	inline hfsbool IsWrite()
	{
		return CUtility::IsWrite(m_mode.c_str());
	}

	inline CFileDriverPtr fp()
	{
		time(&c_time);
		return (CFileDriverPtr)f;
	}

	inline hfststring GetMode()
	{
		time(&c_time);
		return m_mode;
	}
};

typedef Ptr<CKFSHanle> CKFSHanlePtr;

class CKFSHandleMgr:public CRefCounter
{
public:
	hfshandle		AddHandle(CKFSHanlePtr hHandle);
	void			RemoveTimeOutHandle();
	void			RemoveHandle(CKFSHanlePtr hHandle);
	CKFSHanlePtr	GetHandle(hfshandle hHandle);
private:
	map<hfssize,CKFSHanlePtr> m_GlobleHandle;
	G_Lock					  m_Lock;
};

typedef Ptr<CKFSHandleMgr> CKFSHandleMgrPtr;

class CKFSBase:public CBaseInterface
{
public:
	CKFSBase(void);
	~CKFSBase(void);

	virtual hfsint		ReadRecord(vector<TASKDATA*>& taskdata);
	virtual hfsint		ReadRecordLoop(vector<TASKDATA*>& taskdata);
	virtual hfsbool		SetStatus(hfsint64 id,hfsint status);
	virtual hfsbool		WriteResult(hfsint64 id,pchfschar pBuffer,hfsint64 recCount = 0,hfsint64 citeCount=0,hfsint64 cited_count=0);
	hfsint				DoWriteRecord(DOWN_STATE* taskdata);
	virtual hfsbool		LoadZJCLS();
	virtual hfsbool		LoadAuthor();
	virtual hfsbool		LoadHosp();
	virtual hfsbool		LoadUniv();
	virtual void		SetSystemInfo(SYSTEMINFO *systemInfo){m_systemInfo = systemInfo;}
protected:
	CKBConnPtr			GetConn();
	CKBConnPtr			GetConn(SYSTEMINFO * m_systemInfo);
	void				ReleaseConn(CKBConnPtr pCon);
	
	G_Lock				 m_Lock;
private:
	
	vector<GROUPMATCH*>		m_group_info;
	SYSTEMINFO *		m_systemInfo;
};

typedef Ptr<CKFSBase> CKFSBasePtr;
#endif


