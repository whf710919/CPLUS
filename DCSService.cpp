#include "StdAfx.h"
#include "DCSService.h"
#include "Utility.h"
#include <WinInet.h>
#include "folder.h"
#include <sys/stat.h>

#pragma comment (lib,"wininet.lib")
CBasePtr		m_BaseInvoker;
G_Lock			m_gLock;

hfsint CDCSService::m_CurrentTaskThreadNumber = 0;
G_Lock	CDCSService::m_LockThreadNumber;
const char *gStatTable[12] = {"QIKA", "AUTH", "UNIV",  "HOSP", "ZTCS","DOCU","DOWN","STAT","DOWN","COLU","TEXT","CAPJ"};
const char *gMonth[12] = {"01", "02", "03",  "04", "05","06","07","08","09","10","11","12"};

CDCSService::CDCSService(void)
	:m_nTaskMaxThreadsNum(0)
{
	memset(&m_PreExecTime,0,sizeof(struct tm));
	memset(&m_PreExecTimeWeek,0,sizeof(struct tm));
	memset(&m_PreExecTimeDay,0,sizeof(struct tm));
}


CDCSService::~CDCSService(void)
{
}

hfsbool CDCSService::InitInstance()
{
	hfsbool bSuccess = CServiceBase::InitInstance();
	if(!bSuccess){
		return false;
	}
#ifdef HFS_KFS
	m_BaseInvoker = new CKFSBase();
#endif
#ifdef HFS_KDFS
	m_BaseInvoker = new CKFSBase();
#endif

#ifdef HFS_HDFS
	m_BaseInvoker = new CHDFSBase();
#endif

	if(!m_BaseInvoker){
		CUtility::Logger()->trace("m_BaseInvoker new fail");
		return false;
	}

	if ( !InitConfigInfo() )
	{
		CUtility::Logger()->trace("InitConfigInfo fail");
		return false;
	}
	m_BaseInvoker->SetSystemInfo(&m_systemInfo);

	//bSuccess = m_BaseInvoker->LoadZJCLS();
	//if(!bSuccess){
	//	return false;
	//}
	//bSuccess = m_BaseInvoker->LoadAuthor();
	//if(!bSuccess){
	//	return false;
	//}
	//bSuccess = m_BaseInvoker->LoadHosp();
	//if(!bSuccess){
	//	return false;
	//}
	//bSuccess = m_BaseInvoker->LoadUniv();
	//if(!bSuccess){
	//	return false;
	//}

	//取得系统中CPU的数目，最多创建CPU数目*2个线程
	//SYSTEM_INFO SysInfo;
	//GetSystemInfo( &SysInfo );
	//m_nTaskMaxThreadsNum = SysInfo.dwNumberOfProcessors * 2;
	//m_nTaskMaxThreadsNum = SysInfo.dwNumberOfProcessors;
	m_nTaskMaxThreadsNum = 1;//单顺序执行
	return bSuccess;
}

hfsint CDCSService::SetMonth(int nMonth)
{
	int i = 0;
	int iValue = 0;
	char szYear[5];
	memset(szYear,0,sizeof(szYear));
	strncpy(szYear,m_szYearMonth,sizeof(szYear) - 1);
	strcpy(m_szYearMonth,szYear);
	strcat(m_szYearMonth,gMonth[nMonth]);
	strcpy(m_szLocPath,m_szLocDirectory);
	if (m_nAutoDown == 0)
	{
		strcat(m_szLocPath,"\\");
		strcat(m_szLocPath,m_szYearMonth);
	}
	CUtility::Logger()->d("m_szLocPath[%s] m_szYearMonth[%s]",m_szLocPath,m_szYearMonth);
	return 1;
}

hfsint CDCSService::ReadRecord()
{
	//G_Guard Lock(m_Lock);
	hfsint iReadCount = 0;
	CTaskPtr pTask = 0;
	CTaskPtr t(0);
	vector<DOWN_STATE*>	m_taskData;
	hfsint e = 0;
	if (m_nAutoRun)
	{
		if (m_nOncePerMonth == 1)
		{
			if (!IsMayExec())
			{
				return 1;
			}
		}
		else if (m_nOncePerWeek == 1)
		{
			if (!IsMayExecWeek())
			{
				return 1;
			}
		}
		else
		{
			if (!IsMayExecDay())
			{
				return 1;
			}
		}
	}
	//for( int i = REF_RUNQIKAONLY; i <= REF_RUNSTATONLY; i ++ )
	//for( int i = REF_RUNQIKAONLY; i <= REF_RUNQIKAONLY; i ++ )
	//for( int i = REF_RUNAUTHONLY; i <= REF_RUNAUTHONLY; i ++ )
	//for( int i = REF_RUNZTCSONLY; i <= REF_RUNZTCSONLY; i ++ )
	//for( int i = REF_RUNDOCUONLY; i <= REF_RUNDOCUONLY; i ++ )
	//for( int i = REF_RUNUNIVONLY; i <= REF_RUNUNIVONLY; i ++ )
	//for( int i = REF_RUNHOSPONLY; i <= REF_RUNHOSPONLY; i ++ )
	//for( int i = REF_RUNDICTONLY; i <= REF_RUNDICTONLY; i ++ )
	//for( int i = REF_RUNSTATONLY; i <= REF_RUNSTATONLY; i ++ )
	//for( int i = REF_RUNDOWNLOAD; i <= REF_RUNDOWNLOAD; i ++ )
	//for( int i = REF_COLUMNHIERA; i <= REF_COLUMNHIERA; i ++ )
	//REF_CAPJGET
	//int iCount = m_nStatisticVector.size();
	int i = 0;
	int iValue = 0;
	char szYear[5];
	memset(szYear,0,sizeof(szYear));
	strncpy(szYear,m_szYearMonth,sizeof(szYear) - 1);
	CUtility::Logger()->d("szYear[%s] m_szYearMonth[%s]",szYear,m_szYearMonth);
	CUtility::Logger()->d("m_szLocPath[%s] m_szYearMonth[%s] szYear[%s]",m_szLocPath,m_szYearMonth,szYear);
	for( int i = REF_CAPJGET; i <= REF_CAPJGET; i ++ )
	//for (int j = 0; j < iCount; ++j)
	{
		//i = m_nStatisticVector[j];
		DOWN_STATE *item = new DOWN_STATE();
		memset( item, 0, sizeof(DOWN_STATE) );
		item->nStatType = i;
		item->nState = CTask::T_WAIT;
		item->nLog = m_nLog;
		strcpy(item->szIp,m_szIp);
		strcpy(item->szHost,m_szHost);
		strcpy(item->szMsUser,m_szMsUser);
		strcpy(item->szMsPwd,m_szMsPwd);
		strcpy(item->szDbName,m_szDbName);
		item->nPort = m_nPort;
		item->nClearAndInput = m_nClearAndInput;
		item->nDowithErrorZip = m_nDowithErrorZip;
		memset(item->tablename,0,sizeof(item->tablename));
		memset(item->cjfdtablename,0,sizeof(item->cjfdtablename));
		memset(item->cdmdtablename,0,sizeof(item->cdmdtablename));
		memset(item->cpmdtablename,0,sizeof(item->cpmdtablename));
		if ((REF_CAPJGET == i )||(REF_CAPJGET == i ))
		{
			sprintf( item->tablename, "%s_REF1215", gStatTable[i-REF_RUNQIKAONLY] );
			strcpy(item->szLocDirectory,m_szLocDirectory);
			strcpy(item->szLocPath,m_szLocPath);
			item->nAutoDown = m_nAutoDown;
			strcpy(item->szYearMonth,m_szYearMonth);
			item->nStartDayPerMonth = m_nStartDayPerMonth;
			item->nftpPort = m_ftpPort;
			strcpy(item->szUser,m_szUser);
			strcpy(item->szPwd,m_szPwd);
			strcpy(item->szServerDirectory,m_szServerDirectory);
			strcpy(item->szServer,m_szServer);
			strcpy(item->szTableName,m_szTableName);
			strcpy(item->szFieldName,m_szFieldName);

		}
		else if (REF_COLUMNHIERA == i )
		{
			sprintf( item->tablename, "KM%s_HIER", gStatTable[i-REF_RUNQIKAONLY] );
		}
		else
		{
			sprintf( item->tablename, "%s_REF", gStatTable[i-REF_RUNQIKAONLY] );
		}
		if (REF_RUNDOWNLOAD == i )
		{
			strcpy(item->szCjfdCode,m_szCjfdCode);
			strcpy(item->szCdmdCode,m_szCdmdCode);
			strcpy(item->szCpmdCode,m_szCpmdCode);
			strcpy(item->szCheckCpmd,m_szCheckCpmd);

			sprintf( item->cjfdtablename, "%scjfd%s", gStatTable[i-REF_RUNQIKAONLY], m_szYear );

			iValue = _atoi64(m_szYear);
			if ((iValue >= 2005) && (iValue <= 2014))
			{
				sprintf( item->cdmdtablename, "%scdmd%s", gStatTable[i-REF_RUNQIKAONLY], "0514" );
				sprintf( item->cpmdtablename, "%scpmd%s", gStatTable[i-REF_RUNQIKAONLY], "0514" );
			}
			else
			{
				sprintf( item->cdmdtablename, "%scdmd%s", gStatTable[i-REF_RUNQIKAONLY], "1520" );
				sprintf( item->cpmdtablename, "%scpmd%s", gStatTable[i-REF_RUNQIKAONLY], "1520" );
			}
			CUtility::Logger()->d("item->cdmdtablename[%s] item->cpmdtablename[%s] iValue[%d]",item->cdmdtablename,item->cpmdtablename,iValue);
		}
		m_taskData.push_back( item );
	}

	vector<DOWN_STATE*>::iterator it;
	for( it = m_taskData.begin(); it != m_taskData.end(); ++it){
		CTaskPtr pTask(0);
		pTask = new CTask();
		memset(pTask->T(),0,sizeof(DOWN_STATE));
		memcpy(pTask->T(),*it,sizeof(DOWN_STATE));
		e = (hfsint)Task()->Add(pTask);
		delete (*it);
		Sleep(10*1000);
	}
	if (m_nAutoRun == 0)
	{
		return 0;
	}
	return 1;
}

hfsint CDCSService::ReadRecordLoop()
{
	//G_Guard Lock(m_Lock);
	hfsint iReadCount = 0;
	CTaskPtr pTask = 0;
	CTaskPtr t(0);
	vector<DOWN_STATE*>	m_taskData;
	hfsint e = 0;
	//for( int i = REF_RUNQIKAONLY; i <= REF_RUNSTATONLY; i ++ )
	//for( int i = REF_RUNQIKAONLY; i <= REF_RUNQIKAONLY; i ++ )
	//for( int i = REF_RUNAUTHONLY; i <= REF_RUNAUTHONLY; i ++ )
	//for( int i = REF_RUNZTCSONLY; i <= REF_RUNZTCSONLY; i ++ )
	//for( int i = REF_RUNDOCUONLY; i <= REF_RUNDOCUONLY; i ++ )
	//for( int i = REF_RUNUNIVONLY; i <= REF_RUNUNIVONLY; i ++ )
	//for( int i = REF_RUNHOSPONLY; i <= REF_RUNHOSPONLY; i ++ )
	//for( int i = REF_RUNDICTONLY; i <= REF_RUNDICTONLY; i ++ )
	//for( int i = REF_RUNSTATONLY; i <= REF_RUNSTATONLY; i ++ )
	//for( int i = REF_RUNDOWNLOAD; i <= REF_RUNDOWNLOAD; i ++ )
	//for( int i = REF_COLUMNHIERA; i <= REF_COLUMNHIERA; i ++ )
	//REF_CAPJGET
	//int iCount = m_nStatisticVector.size();
	int i = 0;
	int iValue = 0;
	char szYear[5];
	memset(szYear,0,sizeof(szYear));
	strncpy(szYear,m_szYearMonth,sizeof(szYear) - 1);
	CUtility::Logger()->d("szYear[%s] m_szYearMonth[%s]",szYear,m_szYearMonth);
	CUtility::Logger()->d("m_szLocPath[%s] m_szYearMonth[%s] szYear[%s]",m_szLocPath,m_szYearMonth,szYear);
	for( int i = REF_CAPJGETLOOP; i <= REF_CAPJGETLOOP; i ++ )
	//for (int j = 0; j < iCount; ++j)
	{
		//i = m_nStatisticVector[j];
		DOWN_STATE *item = new DOWN_STATE();
		memset( item, 0, sizeof(DOWN_STATE) );
		item->nStatType = i;
		item->nState = CTask::T_WAIT;
		item->nLog = m_nLog;
		strcpy(item->szIp,m_szIp);
		strcpy(item->szHost,m_szHost);
		strcpy(item->szMsUser,m_szMsUser);
		strcpy(item->szMsPwd,m_szMsPwd);
		strcpy(item->szDbName,m_szDbName);
		item->nPort = m_nPort;
		item->nClearAndInput = m_nClearAndInput;
		item->nDowithErrorZip = m_nDowithErrorZip;
		memset(item->tablename,0,sizeof(item->tablename));
		memset(item->cjfdtablename,0,sizeof(item->cjfdtablename));
		memset(item->cdmdtablename,0,sizeof(item->cdmdtablename));
		memset(item->cpmdtablename,0,sizeof(item->cpmdtablename));
		if ((REF_CAPJGETLOOP == i )||(REF_CAPJGETLOOP == i ))
		{
			sprintf( item->tablename, "%s_REF1215", gStatTable[i-REF_RUNQIKAONLY] );
			strcpy(item->szLocDirectory,m_szLocDirectory);
			strcpy(item->szLocPath,m_szLocPath);
			item->nAutoDown = m_nAutoDown;
			strcpy(item->szYearMonth,m_szYearMonth);
			item->nStartDayPerMonth = m_nStartDayPerMonth;
			item->nftpPort = m_ftpPort;
			strcpy(item->szUser,m_szUser);
			strcpy(item->szPwd,m_szPwd);
			strcpy(item->szServerDirectory,m_szServerDirectory);
			strcpy(item->szServer,m_szServer);
			strcpy(item->szTableName,m_szTableName);
			strcpy(item->szFieldName,m_szFieldName);

		}
		else if (REF_COLUMNHIERA == i )
		{
			sprintf( item->tablename, "KM%s_HIER", gStatTable[i-REF_RUNQIKAONLY] );
		}
		else
		{
			sprintf( item->tablename, "%s_REF", gStatTable[i-REF_RUNQIKAONLY] );
		}
		if (REF_RUNDOWNLOAD == i )
		{
			strcpy(item->szCjfdCode,m_szCjfdCode);
			strcpy(item->szCdmdCode,m_szCdmdCode);
			strcpy(item->szCpmdCode,m_szCpmdCode);
			strcpy(item->szCheckCpmd,m_szCheckCpmd);

			sprintf( item->cjfdtablename, "%scjfd%s", gStatTable[i-REF_RUNQIKAONLY], m_szYear );

			iValue = _atoi64(m_szYear);
			if ((iValue >= 2005) && (iValue <= 2014))
			{
				sprintf( item->cdmdtablename, "%scdmd%s", gStatTable[i-REF_RUNQIKAONLY], "0514" );
				sprintf( item->cpmdtablename, "%scpmd%s", gStatTable[i-REF_RUNQIKAONLY], "0514" );
			}
			else
			{
				sprintf( item->cdmdtablename, "%scdmd%s", gStatTable[i-REF_RUNQIKAONLY], "1520" );
				sprintf( item->cpmdtablename, "%scpmd%s", gStatTable[i-REF_RUNQIKAONLY], "1520" );
			}
			CUtility::Logger()->d("item->cdmdtablename[%s] item->cpmdtablename[%s] iValue[%d]",item->cdmdtablename,item->cpmdtablename,iValue);
		}
		m_taskData.push_back( item );
	}

	vector<DOWN_STATE*>::iterator it;
	for( it = m_taskData.begin(); it != m_taskData.end(); ++it){
		CTaskPtr pTask(0);
		pTask = new CTask();
		memset(pTask->T(),0,sizeof(DOWN_STATE));
		memcpy(pTask->T(),*it,sizeof(DOWN_STATE));
		e = (hfsint)Task()->Add(pTask);
		delete (*it);
		Sleep(10*1000);
	}
	if (m_nAutoRun == 0)
	{
		return 0;
	}
	return 1;
}

hfsint CDCSService::DoProcessRecord(DOWN_STATE * m_taskData)
{
	hfsint e = _ERR_INVALID_HANDLE;
	e = m_BaseInvoker->DoWriteRecord(m_taskData);
	return e;
}

hfsbool CDCSService::DoProcessThread()
{
	DWORD dwThread;
	//CUtility::Logger()->d("m_CurrentTaskThreadNumber.[%d] m_nTaskMaxThreadsNum[%d]",m_CurrentTaskThreadNumber,m_nTaskMaxThreadsNum);
	if (m_CurrentTaskThreadNumber < m_nTaskMaxThreadsNum)
	{
		HANDLE h = CreateThread(0,0,TaskThread,this,0,&dwThread);
		Sleep(1000);
		CloseHandle(h);
	}

	return 1;
}

hfsdword __stdcall CDCSService::TaskThread(LPVOID pParam)
{
	MustExec mustexec(AddThreadNumber,MinusThreadNumber);
	hfsbool bWait = true;
	CTaskPtr pTask = Task()->Get();
	if(pTask){
		bWait = false;
		if ((pTask->T()->nStatType == REF_RUNDICTONLY) ||(pTask->T()->nStatType == REF_RUNDOWNLOAD))
		{
			MinusThreadNumber();
		}
		hfsint iResult = DoProcessRecord(pTask->T());
		Task()->Remove(pTask);
		if ((pTask->T()->nStatType == REF_RUNDICTONLY) ||(pTask->T()->nStatType == REF_RUNDOWNLOAD))
		{
			AddThreadNumber();
		}
	}
	return bWait;
}

hfsint CDCSService::RemoveTask(CTaskPtr pTask)
{
	hfsint e = _ERR_INVALID_HANDLE;
	Task()->Remove(pTask);
	return e;
}

hfsint CDCSService::AddThreadNumber()
{
	G_Guard Lock(m_LockThreadNumber);
	return ++m_CurrentTaskThreadNumber;
}

hfsint CDCSService::MinusThreadNumber()
{
	G_Guard Lock(m_LockThreadNumber);
	return --m_CurrentTaskThreadNumber;
}

bool CDCSService::InitConfigInfo()
{
	char m_szTemp[255];
	m_nAutoRun = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "AUTORUN", 1 );
	m_nDowithErrorZip = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "DOWITHERRORZIP", 0 );
	m_nClearAndInput = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "CLEARANDINPUT", 1 );
	m_nOncePerMonth = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "ONCEPERMONTH", 1 );
	m_nDayPerMonth = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "DAYPERMONTH", 1 );
	m_nOncePerWeek = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "ONCEPERWEEK", 1 );
	m_nDayPerWeek = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "DAYPERWEEK", 1 );
	m_nHourDay = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "HOURDAY", 0 );
	m_nMinute = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "MINUTE", 0 );
	m_nLog = CUtility::GetSystemParamInt( "EappGet.ini", "REF", "Log", 0 );
	m_nPort = CUtility::GetSystemParamInt( "EappGet.ini", "KBase", "Port", 4567 );
	CUtility::GetSystemParamString( "EappGet.ini", "KBase", "IP", m_szIp, sizeof(m_szIp), "127.0.0.1" );
	CUtility::GetSystemParamString( "EappGet.ini", "MSSQL", "HOST", m_szHost, sizeof(m_szHost), "127.0.0.1" );
	CUtility::GetSystemParamString( "EappGet.ini", "MSSQL", "USER", m_szMsUser, sizeof(m_szMsUser), "sa" );
	CUtility::GetSystemParamString( "EappGet.ini", "MSSQL", "PWD", m_szMsPwd, sizeof(m_szMsPwd), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "MSSQL", "DBNAME", m_szDbName, sizeof(m_szDbName), "CAPJ" );
	m_nOncePerDay = CUtility::GetSystemParamInt( "EappGet.ini", "RUN", "ONCEPERDAY", 1 );
	//CUtility::GetSystemParamString( "EappGet.ini", "RUN", "STATISTIC", m_szStatistic, sizeof(m_szStatistic), "1,2,3,4,5,6,7" );
	vector<string> items;
	int iCount= 0;
	string strTemp = "";
	//CUtility::Str()->CStrToStrings(m_szStatistic,items,',');
	//int iCount = items.size();
	//if (iCount <= 0)
	//{
	//	return false;
	//}
	//int iValue = 0;
	//for (int i=0; i < iCount; ++i)
	//{
	//	strTemp = items[i];
	//	iValue = _atoi64(strTemp.c_str());
	//	if ((iValue > 0) && (iValue < 9))
	//	{
	//		m_nStatisticVector.push_back(iValue);
	//	}
	//}
	CUtility::GetSystemParamString( "EappGet.ini", "RUN", "TABLENAME", m_szTableName, sizeof(m_szTableName), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "RUN", "FIELDNAME", m_szFieldName, sizeof(m_szFieldName), "" );

	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "SERVER", m_szServer, sizeof(m_szServer), "ftp.cnki.net" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "USER", m_szUser, sizeof(m_szUser), "rizhi" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "PASSWORD", m_szPwd, sizeof(m_szPwd), "rizhixsq" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "SERVERDIRECTORY", m_szServerDirectory, sizeof(m_szServerDirectory), "Download" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "LOCALDIRECTORY", m_szLocDirectory, sizeof(m_szLocDirectory), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "YEARMONTH", m_szYearMonth, sizeof(m_szYearMonth), "" );
	memset(m_szYear,0,sizeof(m_szYear));
	strncpy(m_szYear,m_szYearMonth,sizeof(m_szYear) - 1);
	m_ftpPort = CUtility::GetSystemParamInt( "EappGet.ini", "DOWNLOAD", "PORT", 21 );
	m_nAutoDown = CUtility::GetSystemParamInt( "EappGet.ini", "DOWNLOAD", "AUTODOWN", 0 );
	m_nStartDayPerMonth = CUtility::GetSystemParamInt( "EappGet.ini", "DOWNLOAD", "STARTDAYPERMONTH", 2 );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "WORKPATH", m_szWorkPath, sizeof(m_szWorkPath), "" );
	memset(m_szTemp,0,sizeof(m_szTemp));
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "CJFDCODE", m_szCjfdCode, sizeof(m_szCjfdCode), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "CDMDCODE", m_szCdmdCode, sizeof(m_szCdmdCode), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "CPFDCODE", m_szCpmdCode, sizeof(m_szCpmdCode), "" );
	CUtility::GetSystemParamString( "EappGet.ini", "DOWNLOAD", "CHECKCPFD", m_szCheckCpmd, sizeof(m_szCheckCpmd), "" );
	m_systemInfo.nAutoDown = m_nAutoDown;
	strTemp = m_szLocDirectory;
	//int nLength = strTemp.length();
	//string a = strTemp.substr(strTemp.length() - 1,1);
	if (strTemp.substr(strTemp.length() - 1,1) == "\\")
	{
		strTemp = strTemp.substr(0,strTemp.length()-1);//从0开始
		strcpy(m_szLocDirectory,strTemp.c_str());
	}
	strcpy(m_szLocPath,m_szLocDirectory);
	if (m_nAutoDown == 0)
	{
		strcat(m_szLocPath,"\\");
		strcat(m_szLocPath,m_szYearMonth);
	}
	BOOL bRe = CreateDirectory(m_szLocPath, NULL);
	if(!bRe)
	{
		DWORD err = GetLastError();
		if (err == ERROR_PATH_NOT_FOUND)
		{
			CreateDirectory(m_szLocDirectory, NULL);
			bRe = CreateDirectory(m_szLocPath, NULL);
		}
		if (!bRe)
		{
			err = GetLastError();
			if (err == ERROR_PATH_NOT_FOUND)
			{
				//路径指定错误，已定位到执行目录
				char szFileName[MAX_PATH];
				GetModuleFileName( NULL, szFileName, MAX_PATH );
				strcpy( strrchr( szFileName, '\\' ) + 1, m_szYearMonth );
				strcpy(m_szLocPath,szFileName);
				bRe = CreateDirectory(m_szLocPath, NULL);
				if (!bRe)
				{
					err = GetLastError();
					if (err == ERROR_PATH_NOT_FOUND)
					{
						CUtility::Logger()->trace("CreateDirectory fail %s",m_szLocPath);
						return false;
					}
				}
				strcpy( strrchr( szFileName, '\\' ), "" );
				strcpy(m_szLocDirectory,szFileName);
			}
		}
	}

	char szDate[16]={0};
	CUtility::GetSystemParamString( "EappGet.ini","RUN", "RUNTIME", szDate, sizeof(szDate), "19000101111111" );
	string strDate = szDate;
	if (strDate.length() == 14)
	{
		string strSub = strDate.substr(0,4);
		m_PreExecTime.tm_year	= (int)_atoi64(strSub.c_str()) - 1900;
		strSub = strDate.substr(4,2);
		m_PreExecTime.tm_mon	= (int)_atoi64(strSub.c_str()) - 1;
		strSub = strDate.substr(6,2);
		m_PreExecTime.tm_mday	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(8,2);
		m_PreExecTime.tm_hour	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(10,2);
		m_PreExecTime.tm_min	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(12,2);
		m_PreExecTime.tm_sec	= (int)_atoi64(strSub.c_str());
		//time_t tm1 = mktime(&m_PreExecTime);
	}
	CUtility::GetSystemParamString( "EappGet.ini","RUN", "WEEKRUNTIME", szDate, sizeof(szDate), "19000101111111" );
	strDate = szDate;
	if (strDate.length() == 14)
	{
		string strSub = strDate.substr(0,4);
		m_PreExecTimeWeek.tm_year	= (int)_atoi64(strSub.c_str()) - 1900;
		strSub = strDate.substr(4,2);
		m_PreExecTimeWeek.tm_mon	= (int)_atoi64(strSub.c_str()) - 1;
		strSub = strDate.substr(6,2);
		m_PreExecTimeWeek.tm_mday	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(8,2);
		m_PreExecTimeWeek.tm_hour	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(10,2);
		m_PreExecTimeWeek.tm_min	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(12,2);
		m_PreExecTimeWeek.tm_sec	= (int)_atoi64(strSub.c_str());
		//time_t tm1 = mktime(&m_PreExecTimeWeek);
	}
	CUtility::GetSystemParamString( "EappGet.ini","RUN", "DAYRUNTIME", szDate, sizeof(szDate), "19000101111111" );
	strDate = szDate;
	if (strDate.length() == 14)
	{
		string strSub = strDate.substr(0,4);
		m_PreExecTimeDay.tm_year	= (int)_atoi64(strSub.c_str()) - 1900;
		strSub = strDate.substr(4,2);
		m_PreExecTimeDay.tm_mon	= (int)_atoi64(strSub.c_str()) - 1;
		strSub = strDate.substr(6,2);
		m_PreExecTimeDay.tm_mday	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(8,2);
		m_PreExecTimeDay.tm_hour	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(10,2);
		m_PreExecTimeDay.tm_min	= (int)_atoi64(strSub.c_str());
		strSub = strDate.substr(12,2);
		m_PreExecTimeDay.tm_sec	= (int)_atoi64(strSub.c_str());
	}
	m_systemInfo.nLog = m_nLog;
	m_systemInfo.nPort = m_nPort;
	strcpy(m_systemInfo.szIp,m_szIp);
	return true;
}

hfsbool	CDCSService::IsTimeOn(struct tm* ptm,struct tm* ptmnow)
{
	time_t tNow,tPlan;
	tNow = mktime(ptmnow);
	tPlan = mktime(ptm);
	if (tNow>=tPlan)
	{
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsMayExec()
{
	struct tm ptm;
	struct tm ptmnow;
	time_t now;
	time(&now);
	ptmnow = *localtime(&now);

	if (IsExecuted(&m_PreExecTime,&ptmnow))
	{
		return false;
	}

	struct tm  tmplan;
	memcpy(&tmplan,localtime(&now),sizeof(struct tm));
	tmplan.tm_mday	= m_nDayPerMonth;
	tmplan.tm_hour	= m_nHourDay;
	tmplan.tm_min	= m_nMinute;

	ptm = tmplan;
	if(IsTimeOn(&ptm,&ptmnow)){
		//memcpy(&m_PreExecTime,&ptmnow,sizeof(struct tm));
		m_PreExecTime = ptmnow;
		char szDate[32];
		sprintf( szDate,"%#04d%#02d%#02d%#02d%#02d%#02d",m_PreExecTime.tm_year + 1900 ,
			m_PreExecTime.tm_mon + 1 , m_PreExecTime.tm_mday, m_PreExecTime.tm_hour, m_PreExecTime.tm_min, m_PreExecTime.tm_sec);

		CUtility::WriteSystemParamString( "EappGet.ini",  "RUN", "RUNTIME", szDate );
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsExecuted(struct tm* ptm,struct tm* ptmnow)
{
	if( ( ptmnow->tm_mon <= ptm->tm_mon )&&( ptm->tm_year > 0 ) )
	{
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsExecutedWeek(struct tm ptm,struct tm ptmnow)
{
	if( ( GetMonday(ptmnow) == GetMonday(ptm) )&&( ptm.tm_year > 0 ) )
	{
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsMayExecWeek()
{
	struct tm ptm;
	struct tm ptmnow;
	time_t now;
	time(&now);
	ptmnow = *localtime(&now);

	if (IsExecutedWeek(m_PreExecTimeWeek,ptmnow))
	{
		return false;
	}

	struct tm  tmplan;
	tmplan = ptmnow;
	time_t tMonday;
	tMonday = mktime(&tmplan);
	int week = tmplan.tm_wday;
	if (week == 0) week = 7;
	tMonday -= week*24*3600;
	tMonday += m_nDayPerWeek*24*3600;
	tmplan = *localtime(&tMonday);
	tmplan.tm_hour = m_nHourDay;
	tmplan.tm_min = m_nMinute;

	ptm = tmplan;
	if(IsTimeOn(&ptm,&ptmnow)){
		m_PreExecTimeWeek = ptmnow;
		char szDate[32];
		sprintf( szDate,"%#04d%#02d%#02d%#02d%#02d%#02d",m_PreExecTimeWeek.tm_year + 1900 ,
			m_PreExecTimeWeek.tm_mon + 1 , m_PreExecTimeWeek.tm_mday, m_PreExecTimeWeek.tm_hour, m_PreExecTimeWeek.tm_min, m_PreExecTimeWeek.tm_sec);

		CUtility::WriteSystemParamString( "EappGet.ini",  "RUN", "WEEKRUNTIME", szDate );
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsExecutedDay(struct tm ptm,struct tm ptmnow)
{
	if( ( ptmnow.tm_year == ptm.tm_year )&&(ptmnow.tm_mon == ptm.tm_mon)&&(ptmnow.tm_mday == ptm.tm_mday)&&( ptm.tm_year > 0 ) )
	{
		return true;
	}

	return false;
}

hfsbool	CDCSService::IsMayExecDay()
{
	struct tm ptm;
	struct tm ptmnow;
	time_t now;
	time(&now);
	ptmnow = *localtime(&now);

	if (IsExecutedDay(m_PreExecTimeDay,ptmnow))
	{
		return false;
	}

	struct tm  tmplan;
	tmplan = ptmnow;
	time_t tMonday;
	tMonday = mktime(&tmplan);
	tmplan = *localtime(&tMonday);
	tmplan.tm_hour = m_nHourDay;
	tmplan.tm_min = m_nMinute;

	ptm = tmplan;
	if(IsTimeOn(&ptm,&ptmnow)){
		m_PreExecTimeDay = ptmnow;
		char szDate[32];
		sprintf( szDate,"%#04d%#02d%#02d%#02d%#02d%#02d",m_PreExecTimeDay.tm_year + 1900 ,
			m_PreExecTimeDay.tm_mon + 1 , m_PreExecTimeDay.tm_mday, m_PreExecTimeDay.tm_hour, m_PreExecTimeDay.tm_min, m_PreExecTimeDay.tm_sec);

		CUtility::WriteSystemParamString( "EappGet.ini",  "RUN", "DAYRUNTIME", szDate );
		return true;
	}

	return false;
}

hfsbool	CDCSService::FindServFile()
{
	CUtility::Logger()->d("FindServFile() begin");
	FILETIME fileTime;
	string str,szFile,szFtpInfo;
	WIN32_FIND_DATA findData;
	HINTERNET hFind;
	char szAppName[256];
	strcpy(szAppName,"DownLoadCounter-name");
	HINTERNET hInetSession=InternetOpen(szAppName,INTERNET_OPEN_TYPE_PRECONFIG,NULL,NULL,0);
	HINTERNET hFtpConn=InternetConnect(hInetSession,m_szServer,m_ftpPort,
		m_szUser,m_szPwd,INTERNET_SERVICE_FTP,INTERNET_FLAG_PASSIVE,0);
	if(!hFtpConn)
	{
		InternetCloseHandle(hInetSession);
		::Sleep(10);
		CUtility::Logger()->d("no build ftp connection[%s][%d][%s][%s]",m_szServer,m_ftpPort,m_szUser,m_szPwd);
		return 0;
	}
	DWORD dwLength=MAX_PATH;
	char szFtpDirectory[MAX_PATH]={0};
	strcpy(szFtpDirectory,m_szServerDirectory);
	if(szFtpDirectory!=0)
		FtpSetCurrentDirectory(hFtpConn,szFtpDirectory);
	FtpGetCurrentDirectory(hFtpConn,szFtpDirectory,&dwLength);
    ::SetCurrentDirectory(m_szLocPath);
	if(!(hFind=FtpFindFirstFile(hFtpConn,_T("*"),&findData,0,0)))
	{
		if (GetLastError()  == ERROR_NO_MORE_FILES) 
		{
		}
		::Sleep(10);
		InternetCloseHandle(hFtpConn);
		InternetCloseHandle(hInetSession);
		return 0;
	}
	DWORD dFileSize = 0;
	char szYearMonth[7] = {0};
	char szYearMonthDay[9] = {0};
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cDay[9] = {0};
	sprintf(cDay,"%04d%02d%02d",systime.wYear,systime.wMonth,systime.wDay);

	do{
		if(findData.dwFileAttributes==FILE_ATTRIBUTE_DIRECTORY)
		{
		}
		else
		{
			dFileSize = findData.nFileSizeLow;
			szFile=findData.cFileName;
			strncpy( szYearMonth, findData.cFileName + 1, 6 );
			strncpy( szYearMonthDay, findData.cFileName + 1, 8 );
			if (strcmp(szYearMonth,m_szYearMonth) == 0)
			{
				//当天日期不进入下载列表
				if (strcmp(szYearMonthDay,cDay) != 0)
				{
					m_DownloadFileInfo.insert(make_pair(szFile,dFileSize));
				}
			}
		}
	}while(InternetFindNextFile(hFind,&findData));
	
	InternetCloseHandle(hFind);
	::Sleep(10);
	InternetCloseHandle(hFtpConn);
	InternetCloseHandle(hInetSession);
	CUtility::Logger()->d("FindServFile() end");
	return 1;	
}

bool	CDCSService::CheckLocalFile()
{
	CUtility::Logger()->d("CheckLocalFile() begin");
	WIN32_FIND_DATA findData;
	TCHAR strFilePath[MAX_PATH];
	lstrcpy(strFilePath,m_szLocPath);
	lstrcat(strFilePath,_T("\\*.*"));
	HANDLE hResult = ::FindFirstFile( strFilePath, &findData );
	if(FALSE == (hResult != INVALID_HANDLE_VALUE))
	{
		return false; 
	}
	DWORD dFileSize = 0;
	string szFile = "";
	map<hfsstring,DWORD>::iterator it;
	m_DownloadFileName.clear();
	while(::FindNextFile(hResult,&findData))
	{
		if(findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
		{
		}
		else
		{
			dFileSize = findData.nFileSizeLow;
			szFile=findData.cFileName;
			it = m_DownloadFileInfo.find(szFile);
			if( it != m_DownloadFileInfo.end())
			{
				if (dFileSize < it->second)
				{
					m_DownloadFileName.push_back(szFile);
				}
				it->second = 0;
			}
			else
			{
				continue;
			}
		}
	}
	::FindClose( hResult );
	for( it = m_DownloadFileInfo.begin(); it != m_DownloadFileInfo.end(); ++ it )
	{
		if (it->second > 0)
		{
			m_DownloadFileName.push_back(it->first);
		}
	}
	CUtility::Logger()->d("CheckLocalFile() end");
	return true;
}

hfsbool	CDCSService::DownLoadFile()
{	
	//每30分钟执行一次，由DownLoadThread线程执行
	SYSTEMTIME systime;
	GetLocalTime(&systime);

	if (m_nAutoDown == 1)
	{
		//生成和日期相关的路径，更改m_szLocPath

		if (systime.wMonth == 1)
		{
			sprintf(m_szYearMonth,"%04d%02d%",systime.wYear - 1,systime.wMonth - 1);
		}
		else
		{
			sprintf(m_szYearMonth,"%04d%02d%",systime.wYear,systime.wMonth - 1);
		}

		strcpy(m_szLocPath,m_szLocDirectory);
		strcat(m_szLocPath,"\\");
		strcat(m_szLocPath,m_szYearMonth);
		BOOL bRe = CreateDirectory(m_szLocPath, NULL);
		if(!bRe)
		{
			DWORD err = GetLastError();
			if (err == ERROR_PATH_NOT_FOUND)
			{
				CreateDirectory(m_szLocDirectory, NULL);
				bRe = CreateDirectory(m_szLocPath, NULL);
			}
			if (!bRe)
			{
				err = GetLastError();
				if (err == ERROR_PATH_NOT_FOUND)
				{
					//路径指定错误，已定位到执行目录
					char szFileName[MAX_PATH];
					GetModuleFileName( NULL, szFileName, MAX_PATH );
					strcpy( strrchr( szFileName, '\\' ) + 1, m_szYearMonth );
					strcpy(m_szLocPath,szFileName);
					bRe = CreateDirectory(m_szLocPath, NULL);
					if (!bRe)
					{
						if (err == ERROR_PATH_NOT_FOUND)
						{
							return false;
						}
					}
					strcpy( strrchr( szFileName, '\\' ), "" );
					strcpy(m_szLocDirectory,szFileName);
				}
			}
		}
	}

	Delete2MonthsBeforeData();
	if (m_nAutoDown == 1)
	{
		//生成和日期相关的路径，更改m_szLocPath

		if (systime.wMonth == 1)
		{
			sprintf(m_szYearMonth,"%04d%02d%",systime.wYear - 1,systime.wMonth - 1);
		}
		else
		{
			sprintf(m_szYearMonth,"%04d%02d%",systime.wYear,systime.wMonth - 1);
		}

		strcpy(m_szLocPath,m_szLocDirectory);
		strcat(m_szLocPath,"\\");
		strcat(m_szLocPath,m_szYearMonth);
	}
	if (FindServFile()&&CheckLocalFile())
	{
		if (m_DownloadFileName.size() > 0)
		{
			ReceiveServerFile();
		}
	}
	return true;
}

hfsbool	CDCSService::ReceiveServerFile()
{
	CUtility::Logger()->d("ReceiveServerFile() begin");
	if (m_DownloadFileName.size() <= 0)
	{
		CUtility::Logger()->d("no file may be download");
		return 1;
	}
	string szFile;
	WIN32_FIND_DATA findData;
	HINTERNET hFind;
	char szAppName[256];
	strcpy(szAppName,"DownLoadCounter-name");
	HINTERNET hInetSession=InternetOpen(szAppName,INTERNET_OPEN_TYPE_PRECONFIG,NULL,NULL,0);
	HINTERNET hFtpConn=InternetConnect(hInetSession,m_szServer,m_ftpPort,
		m_szUser,m_szPwd,INTERNET_SERVICE_FTP,INTERNET_FLAG_PASSIVE,0);
	if(!hFtpConn)
	{
		InternetCloseHandle(hInetSession);
		::Sleep(10);
		CUtility::Logger()->d("no build ftp connection[%s][%d][%s][%s]",m_szServer,m_ftpPort,m_szUser,m_szPwd);
		return 0;
	}
	DWORD dwLength=MAX_PATH;
	char szFtpDirectory[MAX_PATH]={0};
	strcpy(szFtpDirectory,m_szServerDirectory);
	if(szFtpDirectory!=0)
		FtpSetCurrentDirectory(hFtpConn,szFtpDirectory);
	FtpGetCurrentDirectory(hFtpConn,szFtpDirectory,&dwLength);
    ::SetCurrentDirectory(m_szLocPath);
	if(!(hFind=FtpFindFirstFile(hFtpConn,_T("*"),&findData,0,0)))
	{
		if (GetLastError()  == ERROR_NO_MORE_FILES) 
		{
		}
		::Sleep(10);
		InternetCloseHandle(hFtpConn);
		InternetCloseHandle(hInetSession);
		CUtility::Logger()->d("path [%s] no exist",szFtpDirectory);
		return 0;
	}
	string strLocalPath="";
	vector<string>::iterator it;
	for( it = m_DownloadFileName.begin(); it != m_DownloadFileName.end(); ++ it )
	{
		strLocalPath = m_szLocPath;
		strLocalPath += "\\";
		strLocalPath += it->c_str();
		if(FtpGetFile(hFtpConn,it->c_str(),strLocalPath.c_str(),FALSE,FILE_ATTRIBUTE_NORMAL,FTP_TRANSFER_TYPE_BINARY |
			INTERNET_FLAG_NO_CACHE_WRITE,0))
		{
		}
		else
		{
			DWORD lError = GetLastError();//ERROR_INTERNET_TIMEOUT
			CUtility::Logger()->d("FtpGetFile downfile[%s] fail errcode[%d]",it->c_str(),lError);
			//szFtpInfo+="出错，退出查找原因";
			break;
		}
	}
	InternetCloseHandle(hFind);
	::Sleep(10);
	InternetCloseHandle(hFtpConn);
	InternetCloseHandle(hInetSession);
	CUtility::Logger()->d("ReceiveServerFile() end");
	return 1;	
}

int  CDCSService::GetMonday(struct tm t)
{
	time_t tMonday;
	tMonday = mktime(&t);
	int week = t.tm_wday;
	if (week == 0) week = 7;
	--week;
	tMonday -= week*24*3600;
	struct tm *pTmp =localtime(&tMonday);
	if (pTmp)
	{
		return pTmp->tm_yday;
	}
	else
	{
		return -1;
	}
}

hfsbool	CDCSService::Delete2MonthsBeforeData()
{	
	SYSTEMTIME systime;
	struct tm ptm;
	GetLocalTime(&systime);
	if (systime.wMonth > 3)
	{
		ptm.tm_year = systime.wYear - 1900;
		ptm.tm_mon = systime.wMonth - 1 -3;
	}
	else
	{
		ptm.tm_year = systime.wYear - 1900 -1;
		ptm.tm_mon = systime.wMonth + 12 - 1 -3;
	}

	if (m_nAutoDown == 1)
	{
		//生成和日期相关的路径，更改m_szLocPath

		sprintf(m_szYearMonth,"%04d%02d%",ptm.tm_year+1900,ptm.tm_mon + 1);

		strcpy(m_szLocPath,m_szLocDirectory);
		strcat(m_szLocPath,"\\");
		strcat(m_szLocPath,m_szYearMonth);

		CFolder f;
		vector<string>strFileList;
		vector<string>::iterator it2;
		f.GetFilesList( m_szLocPath, "*.zip", strFileList );

		for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
		{
			_chmod( it2->c_str(), _S_IWRITE );
			DeleteFile( it2->c_str() );
		}
	}

	return true;
}
