#ifndef HFS_DCSSERVICE_H_
#define HFS_DCSSERVICE_H_
#include "ServiceBase.h"

#include "HandleBase.h"
#include "MustExec.h"
#ifdef HFS_KFS
#include "KFSBase.h"
#endif
#ifdef HFS_KDFS
#include "KFSBase.h"
#endif
#ifdef HFS_HDFS
#include "HDFSBase.h"
#endif

class CDCSService:public CServiceBase
{
public:
	CDCSService(void);
	~CDCSService(void);
	virtual hfsbool		InitInstance();
	hfsint				ReadRecord();
	hfsint				ReadRecordLoop();
	static hfsint		DoProcessRecord(DOWN_STATE * m_taskData);
	hfsbool				DoProcessThread();
	hfsint				RemoveTask(CTaskPtr pTask);
	static	hfsdword __stdcall TaskThread(LPVOID pParam);
	static hfsint		AddThreadNumber();
	static hfsint		MinusThreadNumber();
	static hfsint		m_CurrentTaskThreadNumber;
	static 	G_Lock		m_LockThreadNumber;
	bool				InitConfigInfo();
	bool				IsTimeOn(struct tm* ptm,struct tm* ptmnow);
	bool				IsMayExec();
	bool				IsMayExecWeek();
	bool				IsMayExecDay();
	bool				IsExecuted(struct tm* ptm,struct tm* ptmnow);
	bool				IsExecutedWeek(struct tm ptm,struct tm ptmnow);
	bool				IsExecutedDay(struct tm ptm,struct tm ptmnow);
	bool				DownLoadFile();
	bool				FindServFile();
	void				ServFileDownload();
	bool				CheckLocalFile();
	hfsbool				ReceiveServerFile();
	int					GetMonday(struct tm t);
	bool				Delete2MonthsBeforeData();
	hfsint				SetMonth(int nMonth);
private:
	hfsint					m_nTaskMaxThreadsNum;
	int						m_nAutoRun;
	int						m_nOncePerMonth;
	int						m_nDayPerMonth;
	int						m_nOncePerWeek;
	int						m_nDayPerWeek;
	int						m_nOncePerDay;
	int						m_nHourDay;
	int						m_nMinute;
	int						m_nLog;
	int						m_nPort;
	int						m_nClearAndInput;
	int						m_nDowithErrorZip;
	struct tm				m_PreExecTime;
	struct tm				m_PreExecTimeWeek;
	struct tm				m_PreExecTimeDay;
	struct SYSTEMINFO		m_systemInfo;
	char					m_szServer[32];
	char					m_szUser[32];
	char					m_szPwd[32];
	char					m_szTableName[MAX_FILE_ID];
	char					m_szFieldName[MAX_FILE_ID];
	char					m_szYearMonth[7];
	char					m_szYear[5];
	int						m_ftpPort;
	char					m_szLocDirectory[MAX_PATH];
	char					m_szServerDirectory[MAX_PATH];
	map<hfsstring,DWORD>	m_DownloadFileInfo;
	vector<hfsstring>		m_DownloadFileName;
	int						m_nAutoDown;
	char					m_szLocPath[MAX_PATH];
	int						m_nStartDayPerMonth;
	//char					m_szStatistic[32];
	//vector<int>			m_nStatisticVector;
	char					m_szWorkPath[MAX_PATH];
	char					m_szCjfdCode[MAX_FILE_ID];
	char					m_szCdmdCode[MAX_FILE_ID];
	char					m_szCpmdCode[MAX_FILE_ID];
	char					m_szCheckCpmd[MAX_FILE_ID];
	char					m_szIp[16];
	char					m_szHost[32];
	char					m_szMsUser[32];
	char					m_szMsPwd[32];
	char					m_szDbName[32];
};


#endif