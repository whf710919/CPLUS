#include "stdafx.h"
#include <crtdbg.h>

#include "NTService.h"
#include "Utility.h"

#ifndef RSP_SIMPLE_SERVICE
#define RSP_SIMPLE_SERVICE 1
#endif
#ifndef RSP_UNREGISTER_SERVICE
#define RSP_UNREGISTER_SERVICE 0
#endif


BOOL CNTService::m_bInstance = FALSE;

static CNTService * gpTheService = 0;

CNTService * AfxGetService() { return gpTheService; }


CNTService::CNTService( LPCTSTR lpServiceName, LPCTSTR lpDisplayName )
: m_lpServiceName(lpServiceName)
, m_lpDisplayName(lpDisplayName ? lpDisplayName : lpServiceName)
, m_dwCheckPoint(0)
, m_dwErr(0)
, m_sshStatusHandle(0)
, m_dwControlsAccepted(SERVICE_ACCEPT_STOP)
, m_pUserSID(0)
, m_dwDesiredAccess(SERVICE_ALL_ACCESS)
, m_dwServiceType(SERVICE_WIN32_OWN_PROCESS|SERVICE_INTERACTIVE_PROCESS)
, m_dwStartType(SERVICE_DEMAND_START)
, m_dwErrorControl(SERVICE_ERROR_NORMAL)
, m_pszLoadOrderGroup(0)
, m_pszDependencies(0)
, m_lpCmdLine(0)
{
	_ASSERTE( ! m_bInstance );
	m_bInstance = TRUE;
	gpTheService = this;
	m_ssStatus.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
	m_ssStatus.dwServiceSpecificExitCode = 0;
}


CNTService::~CNTService()
{
	_ASSERTE( m_bInstance );
	delete [] LPBYTE(m_pUserSID);
	m_bInstance = FALSE;
	gpTheService = 0;
}

BOOL CNTService::RegisterService(int type)
{
	BOOL (CNTService::* fnc)() = &CNTService::StartDispatcher;
	
	switch( type)
	{
	case INSTALL:
		fnc = &CNTService::InstallService;
		break;
	case REMOVE:
		fnc = &CNTService::RemoveService;
		break;
	case STARTUP:
		fnc = &CNTService::StartupService;
		break;
	case END:
		fnc = &CNTService::EndService;
		break;
	case CONTINUE:
		fnc = &CNTService::ContinueService;
		break;
	case PAUSE:
		fnc = &CNTService::PauseService;
		break;
		
	}
#ifdef UNICODE
	::GlobalFree((HGLOBAL)Argv);
#endif
	
	return (this->*fnc)();
}

BOOL CNTService::StartDispatcher()
{
	SERVICE_TABLE_ENTRY dispatchTable[] =
    {
        { LPTSTR(m_lpServiceName), (LPSERVICE_MAIN_FUNCTION)ServiceMain },
        { 0, 0 }
    };
	
	BOOL bRet = StartServiceCtrlDispatcher(dispatchTable);
	if( !bRet ){
		hfschar cStr[256];
		GetLastErrorText(cStr, 256);
		//CUtility::Logger()->trace("StartServiceCtrlDispatcher failed - %s", cStr );
	}
	
	return bRet;
}


BOOL CNTService::InstallService()
{
	hfschar cFileName[MAX_PATH];
	hfschar cCmdLine[MAX_PATH];

	GetModuleFileName( 0, cFileName, MAX_PATH);
	strcpy(cCmdLine,cFileName);

	BOOL bRet = FALSE;	
	SC_HANDLE schSCManager = OpenSCManager(0,0,SC_MANAGER_ALL_ACCESS);
	if( m_lpCmdLine){
		strcat(cCmdLine," ");
		strcat(cCmdLine,m_lpCmdLine);
	}
	if( schSCManager )
	{
		SC_HANDLE schService =	CreateService(
			schSCManager,
			m_lpServiceName,
			m_lpDisplayName,
			m_dwDesiredAccess,
			m_dwServiceType,
			m_dwStartType,
			m_dwErrorControl,
			cCmdLine,
			m_pszLoadOrderGroup,
			NULL,
			m_pszDependencies,
			NULL,
			NULL);

		if( schService )
		{
			SERVICE_DESCRIPTION desc;
			desc.lpDescription = (LPSTR)m_lpServiceName;
			ChangeServiceConfig2( schService, SERVICE_CONFIG_DESCRIPTION, &desc );
			
			SERVICE_FAILURE_ACTIONS ac;
			ac.lpRebootMsg = NULL;
			ac.dwResetPeriod = 0;
			ac.lpCommand = NULL;
			ac.cActions = 2;
			SC_ACTION sc[2];

			for( int i = 0; i < 2; i ++ )
			{
				sc[i].Delay = 1000 * 60; 
				sc[i].Type = SC_ACTION_RESTART;
			}
			ac.lpsaActions = sc;

			ChangeServiceConfig2( schService, SERVICE_CONFIG_FAILURE_ACTIONS, &ac );
			
			DWORD len;
			QUERY_SERVICE_CONFIG Config[64];
			if( QueryServiceConfig(schService, Config, sizeof(Config), &len) )
			{
				ChangeServiceConfig( schService, Config[0].dwServiceType, 
									 SERVICE_AUTO_START, Config[0].dwErrorControl, Config[0].lpBinaryPathName,
									 Config[0].lpLoadOrderGroup, NULL, Config[0].lpDependencies,
									 Config[0].lpServiceStartName, NULL, Config[0].lpDisplayName );
			}

			CloseServiceHandle(schService);
			bRet = TRUE;
		}
		else
		{
			LPVOID lpMsgBuf;
			if ( !FormatMessage( 
				FORMAT_MESSAGE_ALLOCATE_BUFFER | 
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
				(LPTSTR) &lpMsgBuf,
				0,
				NULL ) )
			{
				//CUtility::Logger()->trace("CreateService failed ??" );
			}
			else
			{
				//CUtility::Logger()->trace("CreateService failed - %s", lpMsgBuf);
				LocalFree( lpMsgBuf );
			}
		}
		CloseServiceHandle(schSCManager);
	}
	else
	{
		LPVOID lpMsgBuf;
		if ( !FormatMessage( 
			FORMAT_MESSAGE_ALLOCATE_BUFFER | 
			FORMAT_MESSAGE_FROM_SYSTEM | 
			FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
			(LPTSTR) &lpMsgBuf,
			0,
			NULL ) )
		{
			//CUtility::Logger()->trace("CreateService failed ??" );
		}
		else
		{
			//CUtility::Logger()->trace("CreateService failed - %s", lpMsgBuf);
			LocalFree( lpMsgBuf );
		}
	}
	
	if( bRet )
	{
		RegisterApplicationLog(
			cFileName,
			EVENTLOG_ERROR_TYPE | EVENTLOG_WARNING_TYPE | EVENTLOG_INFORMATION_TYPE
			);
		//CUtility::Logger()->trace("%s installed.", m_lpDisplayName );
	}
	
	return bRet;
}


BOOL CNTService::RemoveService()
{
	BOOL bRet = FALSE;
	SC_HANDLE schSCManager = OpenSCManager(0,0,SC_MANAGER_ALL_ACCESS);
	if( schSCManager )
	{
		SC_HANDLE schService =	OpenService(
			schSCManager,
			m_lpServiceName,
			SERVICE_ALL_ACCESS
			);
		
		if( schService )
		{
			if( ControlService(schService, SERVICE_CONTROL_STOP, &m_ssStatus) )
			{
				Sleep(1000);
				
				while( QueryServiceStatus(schService, &m_ssStatus) )
				{
					if( m_ssStatus.dwCurrentState == SERVICE_STOP_PENDING )
						Sleep( 1000 );
					else
						break;
				}
				
				if( m_ssStatus.dwCurrentState != SERVICE_STOPPED ){
					//CUtility::Logger()->trace( "Failed to stop %s.",m_lpServiceName );
				}
			}
			
			if( DeleteService(schService) )
			{
				//CUtility::Logger()->trace( "%s removed.",m_lpServiceName );
			}
			else
			{
				LPVOID lpMsgBuf;
				if ( !FormatMessage( 
					FORMAT_MESSAGE_ALLOCATE_BUFFER | 
					FORMAT_MESSAGE_FROM_SYSTEM | 
					FORMAT_MESSAGE_IGNORE_INSERTS,
					NULL,
					GetLastError(),
					MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
					(LPTSTR) &lpMsgBuf,
					0,
					NULL ) )
				{
					//CUtility::Logger()->trace("DeleteService failed ??" );
				}
				else
				{
					//CUtility::Logger()->trace("DeleteService failed - %s", lpMsgBuf );
					LocalFree( lpMsgBuf );
				}
			}
			
			CloseServiceHandle(schService);
		}
		else
		{
			LPVOID lpMsgBuf;
			if ( !FormatMessage( 
				FORMAT_MESSAGE_ALLOCATE_BUFFER | 
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
				(LPTSTR) &lpMsgBuf,
				0,
				NULL ) )
			{
				//CUtility::Logger()->trace("DeleteService failed ??" );
			}
			else
			{
				//CUtility::Logger()->trace("DeleteService failed - %s", lpMsgBuf );
				LocalFree( lpMsgBuf );
			}
		}
		
		CloseServiceHandle(schSCManager);
	}
	else
	{
		//CUtility::Logger()->trace("OpenSCManager failed .");
	}
	
	if( bRet ){
		DeregisterApplicationLog();
	}
	return bRet;
}


BOOL CNTService::EndService()
{
	BOOL bRet = FALSE;
	SC_HANDLE schSCManager = ::OpenSCManager(0,0,SC_MANAGER_ALL_ACCESS);
	if( schSCManager )
	{
		SC_HANDLE schService =	::OpenService(
			schSCManager,
			m_lpServiceName,
			SERVICE_ALL_ACCESS
			);
		
		if( schService )
		{
			if( ::ControlService(schService, SERVICE_CONTROL_STOP, &m_ssStatus) )
			{
				::Sleep(1000);
				
				while( ::QueryServiceStatus(schService, &m_ssStatus) )
				{
					if( m_ssStatus.dwCurrentState == SERVICE_STOP_PENDING )
						::Sleep( 1000 );
					else
						break;
				}
				
				if( m_ssStatus.dwCurrentState == SERVICE_STOPPED )
				{
					bRet = TRUE;
					//CUtility::Logger()->trace("%s stopped.", m_lpServiceName );
				}
                else
				{
					//CUtility::Logger()->trace("Failed to stop %s.", m_lpDisplayName );
				}
			}
			
			::CloseServiceHandle(schService);
		}
		else
		{
			LPVOID lpMsgBuf;
			if ( !FormatMessage( 
				FORMAT_MESSAGE_ALLOCATE_BUFFER | 
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
				(LPTSTR) &lpMsgBuf,
				0,
				NULL ) )
			{
				//CUtility::Logger()->trace("EndService failed ??" );
			}
			else
			{
				//CUtility::Logger()->trace("EndService failed - %s", lpMsgBuf );
				LocalFree( lpMsgBuf );
			}
		}		
        ::CloseServiceHandle(schSCManager);
    }
	else
	{
		LPVOID lpMsgBuf;
		if ( !FormatMessage( 
			FORMAT_MESSAGE_ALLOCATE_BUFFER | 
			FORMAT_MESSAGE_FROM_SYSTEM | 
			FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
			(LPTSTR) &lpMsgBuf,
			0,
			NULL ) )
		{
			//CUtility::Logger()->trace("EndService failed ??" );
		}
		else
		{
			//CUtility::Logger()->trace("EndService failed - %s", lpMsgBuf );
			LocalFree( lpMsgBuf );
		}
	}
	
	return bRet;
}

BOOL CNTService::PauseService()
{
	BOOL bRet = FALSE;
	SC_HANDLE schSCManager = ::OpenSCManager(0,0,SC_MANAGER_ALL_ACCESS);
	if( schSCManager )
	{
		SC_HANDLE schService =	::OpenService(
			schSCManager,
			m_lpServiceName,
			SERVICE_ALL_ACCESS
			);
		
		if( schService )
		{
			if( ::ControlService(schService, SERVICE_CONTROL_PAUSE, &m_ssStatus) )
			{
				::Sleep(1000);
				
				while( ::QueryServiceStatus(schService, &m_ssStatus) )
				{
					if( m_ssStatus.dwCurrentState == SERVICE_PAUSE_PENDING )
						::Sleep( 1000 );
					else
						break;
				}
				
				if( m_ssStatus.dwCurrentState == SERVICE_PAUSED )
				{
					bRet = TRUE;
					//CUtility::Logger()->trace("%s pause.", m_lpServiceName );
				}
                else
				{
					//CUtility::Logger()->trace("Failed to pause %s.", m_lpDisplayName );
				}
			}
			
			::CloseServiceHandle(schService);
		}
		else
		{
			LPVOID lpMsgBuf;
			if ( !FormatMessage( 
				FORMAT_MESSAGE_ALLOCATE_BUFFER | 
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
				(LPTSTR) &lpMsgBuf,
				0,
				NULL ) )
			{
				//CUtility::Logger()->trace("EndService failed ??" );
			}
			else
			{
				//CUtility::Logger()->trace("PauseService failed - %s", lpMsgBuf );
				LocalFree( lpMsgBuf );
			}
		}
		
        ::CloseServiceHandle(schSCManager);
    }
	else
	{
		LPVOID lpMsgBuf;
		if ( !FormatMessage( 
			FORMAT_MESSAGE_ALLOCATE_BUFFER | 
			FORMAT_MESSAGE_FROM_SYSTEM | 
			FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT), // Default language
			(LPTSTR) &lpMsgBuf,
			0,
			NULL ) )
		{
			//CUtility::Logger()->trace("EndService failed ??" );
		}
		else
		{
			//CUtility::Logger()->trace("PauseService failed - %s", lpMsgBuf );
			LocalFree( lpMsgBuf );
		}
	}
	
	return bRet;
}

BOOL CNTService::ContinueService()
{
	BOOL bRet = FALSE;
	SC_HANDLE schSCManager = ::OpenSCManager(0,0,SC_MANAGER_ALL_ACCESS);
	if( schSCManager )
	{
		SC_HANDLE schService =	::OpenService(
			schSCManager,
			m_lpServiceName,
			SERVICE_ALL_ACCESS
			);
		
		if( schService )
		{
			if( ::ControlService(schService, SERVICE_CONTROL_CONTINUE, &m_ssStatus) )
			{
				::Sleep(1000);
				
				while( ::QueryServiceStatus(schService, &m_ssStatus) )
				{
					if( m_ssStatus.dwCurrentState == SERVICE_CONTINUE_PENDING )
						::Sleep( 1000 );
					else
						break;
				}
				
				if( m_ssStatus.dwCurrentState == SERVICE_RUNNING)
				{
					bRet = TRUE;
					//CUtility::Logger()->trace("%s continue.", m_lpServiceName );
				}
                else
				{
					//CUtility::Logger()->trace("Failed to continue %s.", m_lpServiceName );
				}
			}
			
			::CloseServiceHandle(schService);
		}
		else
		{
			LPVOID lpMsgBuf;
			if ( !FormatMessage( 
				FORMAT_MESSAGE_ALLOCATE_BUFFER | 
				FORMAT_MESSAGE_FROM_SYSTEM | 
				FORMAT_MESSAGE_IGNORE_INSERTS,
				NULL,
				GetLastError(),
				MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
				(LPTSTR) &lpMsgBuf,
				0,
				NULL ) )
			{
				//CUtility::Logger()->trace("ContinueService failed ??" );
			}
			else
			{
				//CUtility::Logger()->trace("ContinueService failed - %s", lpMsgBuf );
				LocalFree( lpMsgBuf );
			}
		}
		
        ::CloseServiceHandle(schSCManager);
    }
	else
	{
		LPVOID lpMsgBuf;
		if ( !FormatMessage( 
			FORMAT_MESSAGE_ALLOCATE_BUFFER | 
			FORMAT_MESSAGE_FROM_SYSTEM | 
			FORMAT_MESSAGE_IGNORE_INSERTS,
			NULL,
			GetLastError(),
			MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
			(LPTSTR) &lpMsgBuf,
			0,
			NULL ) )
		{
			//CUtility::Logger()->trace("ContinueService failed ??" );
		}
		else
		{
			//CUtility::Logger()->trace("ContinueService failed - %s", lpMsgBuf );
			LocalFree( lpMsgBuf );
		}
	}
	
	return bRet;
}

BOOL CNTService::StartupService()
{
	BOOL bRet = FALSE;
	SC_HANDLE schSCManager = ::OpenSCManager(0,	0,SC_MANAGER_ALL_ACCESS);
	if( schSCManager )
	{
		SC_HANDLE schService =	::OpenService(
			schSCManager,
			m_lpServiceName,
			SERVICE_ALL_ACCESS
			);
		
		if( schService )
		{
			if( ::QueryServiceStatus(schService, &m_ssStatus) &&
				m_ssStatus.dwCurrentState == SERVICE_RUNNING ){
					return TRUE;
			}

			if( ::StartService(schService, 0, 0) )
			{
				Sleep(1000);
				
				while( ::QueryServiceStatus(schService, &m_ssStatus) )
				{
					if( m_ssStatus.dwCurrentState == SERVICE_START_PENDING )
						Sleep( 1000 );
					else
						break;
				}
				
				if( m_ssStatus.dwCurrentState == SERVICE_RUNNING )
				{
					bRet = TRUE;
					//CUtility::Logger()->trace("%s started.", m_lpDisplayName );
				}
                else
				{
					//CUtility::Logger()->trace("%s failed to start.", m_lpDisplayName );
				}
			}
			else
			{
				
				//CUtility::Logger()->trace("%s failed to start.", m_lpDisplayName);
			}
			
			::CloseServiceHandle(schService);
		}
		else 
		{
			//CUtility::Logger()->trace("OpenService failed .");
		}
        ::CloseServiceHandle(schSCManager);
    }
	else
	{
		//CUtility::Logger()->trace("OpenSCManager failed.");
	}
	return bRet;
}


void WINAPI CNTService::ServiceMain(DWORD dwArgc, LPTSTR *lpszArgv)
{
	_ASSERTE( gpTheService != 0 );
	gpTheService->m_sshStatusHandle =	RegisterServiceCtrlHandler(gpTheService->m_lpServiceName,CNTService::ServiceCtrl);
	if( gpTheService->m_sshStatusHandle )
	{
		if( gpTheService->ReportStatus(SERVICE_START_PENDING) )
		{
			gpTheService->Run( dwArgc, lpszArgv );
		}
	}
	
	if( gpTheService->m_sshStatusHandle )
		gpTheService->ReportStatus(SERVICE_STOPPED);
}

void WINAPI CNTService::ServiceCtrl(DWORD dwCtrlCode)
{
	_ASSERTE( gpTheService != 0 );

	switch( dwCtrlCode )
	{
	case SERVICE_CONTROL_STOP:
		gpTheService->m_ssStatus.dwCurrentState = SERVICE_STOP_PENDING;
		gpTheService->Stop();
		break;
		
	case SERVICE_CONTROL_PAUSE:
		gpTheService->m_ssStatus.dwCurrentState = SERVICE_PAUSE_PENDING;
		gpTheService->Pause();
		break;
		
	case SERVICE_CONTROL_CONTINUE:
		gpTheService->m_ssStatus.dwCurrentState = SERVICE_CONTINUE_PENDING;
		gpTheService->Continue();
		break;
		
	case SERVICE_CONTROL_SHUTDOWN:
		gpTheService->Shutdown();
		break;
		
	case SERVICE_CONTROL_INTERROGATE:
		gpTheService->ReportStatus(gpTheService->m_ssStatus.dwCurrentState);
		break;
		
	default:
		break;
	}	
}

BOOL CNTService::SetCmdLine(LPCTSTR lpCmdLine)
{
	m_lpCmdLine = lpCmdLine;
	return TRUE;
}

BOOL WINAPI CNTService::ControlHandler(DWORD dwCtrlType) {
	_ASSERTE(gpTheService != 0);
	switch( dwCtrlType ) {
	case CTRL_BREAK_EVENT: 
	case CTRL_C_EVENT: 
		{
			//CUtility::Logger()->trace("Stopping %s.", gpTheService->m_lpDisplayName );
			gpTheService->Stop();
			return TRUE;
		}
	}
	return FALSE;
}

BOOL CNTService::ReportStatus(
							  DWORD dwCurrentState,
							  DWORD dwWaitHint,
							  DWORD dwErrExit ) {
	BOOL fResult = TRUE;
	if( dwCurrentState == SERVICE_START_PENDING)
		m_ssStatus.dwControlsAccepted = 0;
	else
		m_ssStatus.dwControlsAccepted = m_dwControlsAccepted;
	
	m_ssStatus.dwCurrentState = dwCurrentState;
	m_ssStatus.dwWin32ExitCode = NO_ERROR;
	m_ssStatus.dwWaitHint = dwWaitHint;
	
	m_ssStatus.dwServiceSpecificExitCode = dwErrExit;
	if (dwErrExit!=0)
		m_ssStatus.dwWin32ExitCode = ERROR_SERVICE_SPECIFIC_ERROR;
	
	if( dwCurrentState == SERVICE_RUNNING ||
		dwCurrentState == SERVICE_STOPPED )
		m_ssStatus.dwCheckPoint = 0;
	else
		m_ssStatus.dwCheckPoint = ++m_dwCheckPoint;
	
	if (!(fResult = SetServiceStatus( m_sshStatusHandle, &m_ssStatus))) 
	{
		//CUtility::Logger()->trace( "CNTService::ReportStatus-SetServiceStatus() failed" );
	}
	
    return fResult;
}

LPTSTR CNTService::GetLastErrorText( LPTSTR lpszBuf, DWORD dwSize )
{
    LPTSTR lpszTemp = 0;
	
    DWORD dwRet =	::FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM |FORMAT_MESSAGE_ARGUMENT_ARRAY,
		0,
		GetLastError(),
		LANG_NEUTRAL,
		(LPTSTR)&lpszTemp,
		0,
		0
		);
	
    if( !dwRet || (dwSize < dwRet+14) )
        lpszBuf[0] = TEXT('\0');
    else {
        lpszTemp[_tcsclen(lpszTemp)-2] = TEXT('\0');
        _tcscpy(lpszBuf, lpszTemp);
    }
	
    if( lpszTemp )
        LocalFree(HLOCAL(lpszTemp));
	
    return lpszBuf;
}

void CNTService::RegisterApplicationLog( LPCTSTR lpszFileName, DWORD dwTypes )
{
	TCHAR szKey[256];
	_tcscpy(szKey, gszAppRegKey);
	_tcscat(szKey, m_lpServiceName);
	HKEY hKey = 0;
	LONG lRet = ERROR_SUCCESS;
	
	if( ::RegCreateKey(HKEY_LOCAL_MACHINE, szKey, &hKey) == ERROR_SUCCESS )
	{
		lRet =	::RegSetValueEx(
			hKey,
			TEXT("EventMessageFile"),
			0,	
			REG_EXPAND_SZ,
			(CONST BYTE*)lpszFileName,
			_tcslen(lpszFileName) + 1
			);
		lRet =	::RegSetValueEx(
			hKey,
			TEXT("TypesSupported"),
			0,
			REG_DWORD,
			(CONST BYTE*)&dwTypes,
			sizeof(DWORD)
			);

		::RegCloseKey(hKey);
	}
	
	
	lRet =	::RegOpenKeyEx( 
		HKEY_LOCAL_MACHINE,
		gszAppRegKey,
		0,
		KEY_ALL_ACCESS,
		&hKey
		);
	if( lRet == ERROR_SUCCESS ) {
		DWORD dwSize;
		
		lRet =	::RegQueryValueEx(
			hKey,
			TEXT("Sources"),
			0,	
			0,
			0,
			&dwSize
			);
		
		if( lRet == ERROR_SUCCESS ) {
			DWORD dwType;
			DWORD dwNewSize = dwSize+_tcslen(m_lpServiceName)+1;
			LPBYTE Buffer = LPBYTE(::GlobalAlloc(GPTR, dwNewSize));
			
			lRet =	::RegQueryValueEx(
				hKey,
				TEXT("Sources"),
				0,
				&dwType,
				Buffer,
				&dwSize
				);
			if( lRet == ERROR_SUCCESS ) {
				_ASSERTE(dwType == REG_MULTI_SZ);
				register LPTSTR p = LPTSTR(Buffer);
				for(; *p; p += _tcslen(p)+1 ) {
					if( _tcscmp(p, m_lpServiceName) == 0 )
						break;
				}
				if( ! * p ) {
					_tcscpy(p, m_lpServiceName);
					lRet =	::RegSetValueEx(
						hKey,
						TEXT("Sources"),
						0,
						dwType,
						Buffer,
						dwNewSize
						);
				}
			}
			
			::GlobalFree(HGLOBAL(Buffer));
		}
		
		::RegCloseKey(hKey);
	}
}


void CNTService::DeregisterApplicationLog() {
	TCHAR szKey[256];
	_tcscpy(szKey, gszAppRegKey);
	_tcscat(szKey, m_lpServiceName);
	HKEY hKey = 0;
	LONG lRet = ERROR_SUCCESS;
	
	lRet = ::RegDeleteKey(HKEY_LOCAL_MACHINE, szKey);

	lRet =	::RegOpenKeyEx( 
		HKEY_LOCAL_MACHINE,
		gszAppRegKey,
		0,
		KEY_ALL_ACCESS,
		&hKey
		);
	if( lRet == ERROR_SUCCESS ) {
		DWORD dwSize;

		lRet =	::RegQueryValueEx(
			hKey,
			TEXT("Sources"), 
			0,
			0, 
			0,
			&dwSize
			);
		
		if( lRet == ERROR_SUCCESS ) {
			DWORD dwType;
			LPBYTE Buffer = LPBYTE(::GlobalAlloc(GPTR, dwSize));
			LPBYTE NewBuffer = LPBYTE(::GlobalAlloc(GPTR, dwSize));
			
			lRet =	::RegQueryValueEx(
				hKey,
				TEXT("Sources"),
				0,
				&dwType,
				Buffer,
				&dwSize
				);
			if( lRet == ERROR_SUCCESS ) {
				_ASSERTE(dwType == REG_MULTI_SZ);
				
				register LPTSTR p = LPTSTR(Buffer);
				register LPTSTR pNew = LPTSTR(NewBuffer);
				BOOL bNeedSave = FALSE;
				for(; *p; p += _tcslen(p)+1) {
					if( _tcscmp(p, m_lpServiceName) != 0 ) {
						_tcscpy(pNew, p);
						pNew += _tcslen(pNew)+1;
					} else {
						bNeedSave = TRUE;
						dwSize -= _tcslen(p)+1;
					}
				}
				if( bNeedSave ) {
					lRet =	::RegSetValueEx(
						hKey,
						TEXT("Sources"),
						0,
						dwType,
						NewBuffer,
						dwSize
						);
				}
			}
			
			::GlobalFree(HGLOBAL(Buffer));
			::GlobalFree(HGLOBAL(NewBuffer));
		}
		
		::RegCloseKey(hKey);
	}
}