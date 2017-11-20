#include "StdAfx.h"
#include "server.h"
#include "hashtemp.h"
#include <sys/stat.h>
#include <io.h>
#include "Error.h"
#include "Utility.h"

#include "DCSService.h"

CServer::CServer()
	:m_dwServiceMode(REF_SERVICE)
{
	m_bRun = FALSE;
}

CServer::~CServer()
{
}

int CServer::Init(DWORD dwService)
{
	m_bRun = TRUE;
	
	CUtility::Config()->SetServiceType(dwService);

	if(dwService == REF_SERVICE){
		m_ServiceInvoker = new CDCSService();
		if(!m_ServiceInvoker){
			return _ERR_INIT_REF;
		}
		if(!m_ServiceInvoker->InitInstance()){
			return _ERR_INIT_REF;
		}
	}

	DWORD dwThread = NULL;
	HANDLE h = NULL;
	//h = CreateThread(0,0,DownLoadThread,this,0,&dwThread);
	//if(h){
	//	m_hThread.push_back(h);
	//}
	h = CreateThread(0,0,ReadThread,this,0,&dwThread);
	if(h){
		m_hThread.push_back(h);
	}
	h = CreateThread(0,0,TaskThread,this,0,&dwThread);
	if(h){
		m_hThread.push_back(h);
	}
	return 0;
}



hfsdword __stdcall CServer::ReadThread(LPVOID pParam)
{
	CServer *pThis = (CServer*)pParam;
	//for( int j = 2; j < 4; j ++ )
	//{	
		//pThis->m_ServiceInvoker->SetMonth(j);
		while( pThis->IsRun() )
		{
			CUtility::Logger()->d("ReadRecord begin");
			int iReadCount = pThis->m_ServiceInvoker->ReadRecord();
			CUtility::Logger()->d("ReadRecord end");
			if (iReadCount == 0)
			{
				CUtility::Logger()->d("EXIT LOOP");
				break;
			}
			Sleep(5*60*1000);//5分钟间隔
			CUtility::Logger()->d("ReadRecordLoop begin");
			iReadCount = pThis->m_ServiceInvoker->ReadRecordLoop();
			CUtility::Logger()->d("ReadRecordLoop end");
			Sleep(5*60*1000);//5分钟间隔
		}
	//}

	return 1;
}

hfsdword __stdcall CServer::DownLoadThread(LPVOID pParam)
{
	CServer *pThis = (CServer*)pParam;
	while( pThis->IsRun() )
	{
		pThis->m_ServiceInvoker->DownLoadFile();
		Sleep(30*60*1000);//30分钟间隔
		//Sleep(30*1000);//30秒间隔
	}

	return 1;
}

hfsdword __stdcall CServer::TaskThread(LPVOID pParam)
{
	CServer *pThis = (CServer*)pParam;
	while( pThis->IsRun() )
	{
		hfsbool bWait = true;
		bWait = pThis->m_ServiceInvoker->DoProcessThread();
		if(bWait){
			Sleep(500);
		}
	}

	return 1;
}
