#include "StdAfx.h"
#include "EappGet.h"
#include "Server.h"
#include "Error.h"
#include "Utility.h"

CRefCount::CRefCount(pchfschar pServiceName, pchfschar pDisplayName)
	: CNTService(pServiceName, pDisplayName)
	, m_hStop(0)
{
	m_dwControlsAccepted = 0;
	m_dwControlsAccepted |= SERVICE_ACCEPT_STOP;
	m_dwControlsAccepted |= SERVICE_ACCEPT_PAUSE_CONTINUE;
	m_dwControlsAccepted |= SERVICE_ACCEPT_SHUTDOWN;
	m_dwServiceType |= SERVICE_INTERACTIVE_PROCESS;
}

CRefCount::~CRefCount(void)
{
	if( m_hStop )
		::CloseHandle(m_hStop);
}

void CRefCount::Run(DWORD argc, LPTSTR * argv)
{
	
	ReportStatus(SERVICE_START_PENDING);

	m_hStop = ::CreateEvent( 0, TRUE, FALSE, 0 );
	hfsint e = _ERR_SUCCESS;

	CUtility::Logger()->trace("service name:%s",m_lpServiceName);
	m_pServer = new CServer();
	if( !m_pServer )
	{
		CUtility::Logger()->trace("m_pServer new fail");
		return;
	}
	CUtility::Logger()->trace("m_pServer new success");

	if( m_pServer->Init(REF_SERVICE) < 0 )
	{
		CUtility::Logger()->trace("m_pServer init fail");
		return;
	}
	CUtility::Logger()->trace("m_pServer init success");

	ReportStatus( SERVICE_RUNNING );
	while( ::WaitForSingleObject(m_hStop, 200) != WAIT_OBJECT_0 )
	{

	}

	if( m_hStop ){
		::CloseHandle(m_hStop);
	}
}

void CRefCount::Stop()
{
	if( m_hStop )
	{
		::SetEvent(m_hStop);
	}
}

void CRefCount::Pause()
{
	Stop();
}

void CRefCount::Continue()
{
	Run(0, NULL);
}

void CRefCount::Shutdown()
{
	Stop();
}
