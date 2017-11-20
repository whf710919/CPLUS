#ifndef HFS_HANDLEBASE_H_
#define HFS_HANDLEBASE_H_
#include "DataDef.h"
#include "smartptr.h"
#include "GLock.h"

#define		HFS_MEM_MAGIC_ID			0x0617ABCD
#define		HFS_INVALIDATE_ID			0xFFFFFFFF
#define		HANDLE_TIMEOUT_SEC			(5*60)

class CHandleBase: public CRefCounter
{
public:
	CHandleBase(void);
	CHandleBase(hfshandle hHandle, pchfschar pServer, hfsint Port);
	CHandleBase(vector<hfsint>* handles);
	CHandleBase(CRefCounterPtr smart);
	~CHandleBase(void);
	hfsbool			IsPtr();
	hfsint			Close();
	hfsbool			IsTimeOut();
	void			Refresh();
	hfshandle		GetHandle();
	hfsstring		GetServer();
	hfsint			GetPort();

	inline operator hfshandle()
	{
		//Refresh();
		return (hfshandle)(void*)this;
	}

	inline operator void*()
	{
		//Refresh();
		return this;;
	}

	inline CRefCounterPtr smart()
	{
		return m_SmartHandle;
	}

	inline vector<hfsint>* nodes()
	{
		return &m_Handles;
	}

private:
	hfshandle		m_dwMagicID;		
	hfshandle		m_dwAddress;
	time_t			m_ctime;
	hfshandle		m_hHandle;
	hfsstring		m_Server;
	hfsint			m_Port;
	CRefCounterPtr	m_SmartHandle;
	vector<hfsint>m_Handles;

};

typedef Ptr<CHandleBase> CHandleBasePtr;

class CHandleBaseMap:public CRefCounter
{
public:
	CHandleBaseMap(hfsint maxSize);
	~CHandleBaseMap();
	hfshandle			AddHandle(void* ptr);
	CHandleBasePtr		GetHandle(hfshandle hHandle);
	void				RemoveHandle(hfshandle hHandle);
private:
	map<hfssize,CHandleBasePtr> m_HandleCache;
	G_Lock				m_Lock;
	hfsint				m_MaxSize;
};

typedef Ptr<CHandleBaseMap> CHandleBaseMapPtr;

#endif
