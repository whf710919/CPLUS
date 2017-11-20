#ifndef HFS_SERVER_H_
#define HFS_SERVER_H_

#include "GLock.h"
#include "hashtemp.h"
#include "DataDef.h"
#include <map>
#define EVENT_NUM   2
#define EVENT_EXIT	0
#define EVENT_TASK	1
#define EVENT_CMD   2
#define EVENT_COMMON 3
#include "stdafx.h"


#include <time.h>

#include <sys/stat.h>
#include "DataDef.h"
#include "KFSBase.h"
#include "ServiceBase.h"

#define MAX_RETRY_TIMES 1

class CServer; 


#define INVALID_CMD_CODE 0xffff

typedef int (CServer::* TPI_PNETCMD)(const char*, int, char *, int *);

struct TPI_NETCMD_MAP_ENTRY
{
	WORD wCmdCode;
	WORD wSubCmdCode;
	TPI_PNETCMD pfn;
};

#define DECLARE_NETCMD_MAP() \
private: \
	static const TPI_NETCMD_MAP_ENTRY m_NetCmd_Entries[];

#define BEGIN_NETCMD_MAP(theClass) \
	const TPI_NETCMD_MAP_ENTRY theClass::m_NetCmd_Entries[] = \
{ 

#define END_NETCMD_MAP( errFunc ) \
{ INVALID_CMD_CODE, INVALID_CMD_CODE, errFunc } \
};

#	define ON_NET_CMD( wNetCmdCode, wNetSubCmdCode, memberFxn ) \
{  wNetCmdCode, wNetSubCmdCode, memberFxn },

class CServer
{
public:
	CServer();
	virtual ~CServer();

	int		Init(DWORD dwService);
	BOOL	IsRun(){ G_Guard Lock(m_Lock);return m_bRun; };
	static	hfsdword __stdcall ReadThread(LPVOID pParam);
	static	hfsdword __stdcall TaskThread(LPVOID pParam);
	static	hfsdword __stdcall DownLoadThread(LPVOID pParam);

public:
	DWORD		m_dwServiceMode;
	BOOL		m_bRun;
private:
	vector<HANDLE>		m_hThread;
	G_Lock				m_Lock;
	//CBasePtr			m_BaseInterface;
	CServiceBasePtr		m_ServiceInvoker;	
};

#endif