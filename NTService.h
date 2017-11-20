#include "stdafx.h"
#include <Winsvc.h>

#ifndef NTService_h
#define NTService_h

class CNTService
{
	static BOOL				m_bInstance;
	
	protected:
		LPCTSTR					m_lpServiceName;
		LPCTSTR					m_lpCmdLine;
		LPCTSTR					m_lpDisplayName;
		DWORD					m_dwCheckPoint;
		DWORD					m_dwErr;
		SERVICE_STATUS			m_ssStatus;
		SERVICE_STATUS_HANDLE	m_sshStatusHandle;
		DWORD					m_dwControlsAccepted;
		PSID					m_pUserSID;
		DWORD			m_dwDesiredAccess;
		DWORD			m_dwServiceType;
		DWORD			m_dwStartType;
		DWORD			m_dwErrorControl;
		LPCTSTR			m_pszLoadOrderGroup;
		LPCTSTR			m_pszDependencies;
	public:
		CNTService(LPCTSTR ServiceName, LPCTSTR DisplayName = 0);
		~CNTService();

	private:
		CNTService( const CNTService & );
		CNTService & operator=( const CNTService & );

	public:	
		virtual void	Run(DWORD argc, LPTSTR * argv) = 0;
		virtual void	Stop() = 0;
		virtual void	Pause()=0;
		virtual void	Continue()=0;
		virtual void	Shutdown()=0;
		enum{INSTALL=1, REMOVE, STARTUP, END, PAUSE, CONTINUE, MYDEBUG};
		virtual BOOL	RegisterService(int type);
		virtual BOOL	StartDispatcher();
		virtual BOOL	InstallService();
		virtual BOOL	RemoveService();
		virtual BOOL	EndService();
		virtual BOOL	PauseService();
		virtual BOOL	ContinueService();
		virtual BOOL	StartupService();
		virtual	BOOL	SetCmdLine(LPCTSTR lpCmdLine);
	protected:
		virtual void	RegisterApplicationLog(
							LPCTSTR lpszProposedMessageFile,
							DWORD dwProposedTypes
						);
		virtual void	DeregisterApplicationLog();

	public:	

		LPTSTR			GetLastErrorText(LPTSTR Buf, DWORD Size);
		BOOL			ReportStatus(
							DWORD CurState,
							DWORD WaitHint = 3000,
							DWORD ErrExit = 0
						);

	public:	
		static void WINAPI	ServiceCtrl(DWORD CtrlCode);
		static void WINAPI	ServiceMain(DWORD argc, LPTSTR * argv);
		static BOOL WINAPI	ControlHandler(DWORD CtrlType);
};

CNTService * AfxGetService();
const LPCTSTR gszAppRegKey = TEXT("SYSTEM\\CurrentControlSet\\Services\\EventLog\\Application\\");

#endif
