#include "StdAfx.h"
#include "Error.h"
#include "kbase.h"
#include "Utility.h"
#include "RecFile.h"
#include "SmartBuffer.h"
#include <math.h>
#include "folder.h"
#include "XUnzip.h"
#include "hashtemp.h"
#include <WinInet.h>
#include <sys/stat.h>
#include "stdio.h"
#include "mssql.h"

#pragma comment (lib,"wininet.lib")

int cmp(const pair<string,int>& x,const pair<string,int>& y);
void sortMapByValue(map<string,int>& tMap,vector<pair<string,int>>& tVector);
extern SYSTEM_TABLE_FIELD_STYLE NODEVALUE[];
extern vector<ST_ZJCLS *> m_ZJCls;
extern vector<ST_AUTHOR *> m_Author;
extern const char *gMonth[12];
const char *gZj[10] = {"A", "B", "C", "D", "E","F","G","H","I","J"};
const char *gCs1[5] = {"汉语字典", "汉语词典", "双语词典",  "专科辞典", "百科全书"};
const char *gCs2[4] = {"汉语字典", "汉语词典", "专科辞典", "百科全书"};

#define GETRECORDNUM 100
#ifdef _WIN_HFS_32
#pragma comment(lib, "..\\lib\\x86\\tpiclient.lib")
#else
#pragma comment(lib, "..\\lib\\x64\\tpiclient.lib")
#endif
//const char *g_KBaseUID="App Admin\tCount\nIs Dbown";
const char *g_KBaseUID="DBOWN";
CDirFactoryPtr G_DirFactory = new CDirFactory();

#define MAX_VIEW_TABLES 500

CDirFactory::CDirFactory()
{
	m_Con = new CKBConn("127.0.0.1",4567);
}

CDirFactory::~CDirFactory()
{
	
}

hfsbool CDirFactory::DirExist(pchfschar pTable)
{
	G_Guard Lock(m_Lock);
	
	map<hfsstring,HFS_DIR_INFO*>::iterator it;

	hfsstring sDirName = m_Con->GetFileTable(pTable);
	hfschar   cDirName[MAX_FILE_ID];
	strcpy(cDirName,sDirName.c_str());
	_strupr(cDirName);

	it = m_dirs.find(cDirName);
	if( it != m_dirs.end()){
		return true;
	}

	return false;
}

void CDirFactory::CreateDir(pchfschar pTable,HFS_DIR_INFO* dir)
{
	if(DirExist(pTable)){//refresh
		DeleteDir(pTable);
	}

	HFS_DIR_INFO* pdir = new HFS_DIR_INFO;
	memset(pdir,0x00,sizeof(HFS_DIR_INFO));

	hfsstring sDirName = m_Con->GetFileTable(pTable);
	hfschar   cDirName[MAX_FILE_ID];
	strcpy(cDirName,sDirName.c_str());
	_strupr(cDirName);
	
	G_Guard Lock(m_Lock);

	if(dir){
		memcpy(pdir,dir,sizeof(HFS_DIR_INFO));
		m_dirs.insert(make_pair(cDirName,pdir));
	}else{
		if(m_Con->GetTableInfoX(cDirName,pdir) >= _ERR_SUCCESS){
			m_dirs.insert(make_pair(cDirName,pdir));
		}
	}
}

void CDirFactory::DeleteDir(pchfschar pTable)
{
	hfsstring sDirName = m_Con->GetFileTable(pTable);
	hfschar   cDirName[MAX_FILE_ID];
	strcpy(cDirName,sDirName.c_str());
	_strupr(cDirName);

	G_Guard Lock(m_Lock);

	map<hfsstring,HFS_DIR_INFO*>::iterator it;
	it = m_dirs.find(cDirName);
	if( it != m_dirs.end()){
		delete it->second;

		m_dirs.erase(it);
	}
}

hfsbool CDirFactory::GetDirCacheInfo(pchfschar pTable,HFS_DIR_INFO* dir)
{
	hfsstring sDirName = m_Con->GetFileTable(pTable);
	hfschar   cDirName[MAX_FILE_ID];
	strcpy(cDirName,sDirName.c_str());
	_strupr(cDirName);

	G_Guard Lock(m_Lock);
	map<hfsstring,HFS_DIR_INFO*>::iterator it;
	it = m_dirs.find(cDirName);
	if( it != m_dirs.end()){
		memcpy(dir,it->second,sizeof(HFS_DIR_INFO));
		return true;
	}

	return false;
}


/*
	@function:		CKBConn
	@description:	初始化KBASE连接对象
	@calls:			
	@called by:		KFSBase.cpp
	@param	pSvrIP	[in]	服务器名字
	@parm	uiPort	[in]	服务器端口	
*/
CKBConn::CKBConn(pchfschar pSvrIP, hfsuint uiPort)
{
	m_hConn = NULL;

	m_lCount = 0;
	m_nPort = uiPort;
	m_hSet = 0;
	m_ErrCode = _ERR_SUCCESS;
	strcpy( m_szIP, pSvrIP );
	m_bExcept = false;

	strcpy( m_OutFileName.szFN_QIKA , "QIKA_REF" ) ;
    strcpy( m_OutFileName.szFN_HOSP , "HOSP_REF" );
    strcpy( m_OutFileName.szFN_UNIV , "UNIV_REF" );
    strcpy( m_OutFileName.szFN_AUTH , "AUTH_REF" );
    strcpy( m_OutFileName.szFN_ZTCS , "ZTCS_REF" );
	strcpy( m_OutFileName.szFN_DOCU , "DOCU_REF" );
	strcpy( m_OutFileName.szFN_DOCU , "DOWN_REF1215" );
	m_ReUpData = 0;
	memset( m_szZSK, 0, sizeof(m_szZSK) );
}


CKBConn::~CKBConn()
{
	Clear();
	CloseConn();
}

int CKBConn::OpenConn()
{
	TPI_LOGIN_PARA login;
	strcpy( login.UserIp.szIp, "127.0.0.1" );
	strcpy( login.szUserName, g_KBaseUID );
	strcpy( login.szPassWord, "" );

	TPI_HCON hConn = TPI_OpenCon( m_szIP, m_nPort, login, &m_ErrCode );
	if( m_ErrCode < 0 )
		return m_ErrCode;

	m_hConn = hConn; 

	return m_ErrCode;
}

void CKBConn::CloseConn()
{
	m_lCount = 0;
	if( m_hConn != NULL )
		TPI_CloseCon( m_hConn );
	m_hConn = NULL;
}


int CKBConn::IsConnected()
{
	m_ErrCode = _ERR_SUCCESS;
	for( int i = 0; i < WAIT_TIMEOUT_SEC; i ++ )
	{
		if( m_hConn == NULL ){
			m_ErrCode = OpenConn();
		}
		else
		{
			m_ErrCode = TPI_IsActiveCon( m_hConn ); 
			if( m_ErrCode < 0 )
			{
				while( m_lCount > 0 )
					Sleep( 10 );

				m_ErrCode = TPI_Reconnect( m_hConn ); 
				if( m_ErrCode < 0 )
				{
					CloseConn();
				}
			}
		}
		if(m_ErrCode >= _ERR_SUCCESS){
			break;
		}

		Sleep(100);
	}
	if(!m_hConn){
		return _ERR_KBASE_CONN;
	}
	return m_ErrCode;
}

hfsint CKBConn::Clear()
{
	if( m_hSet ){
		m_ErrCode = TPI_CloseRecordSet(m_hSet);
		if(m_ErrCode < 0 ){
			if( (m_ErrCode = IsConnected()) >= _ERR_SUCCESS){
				TPI_CloseRecordSet(m_hSet);
			}
		}
	}
	m_ErrCode = _ERR_SUCCESS;
	m_hSet = 0;
	return m_ErrCode;
}

int CKBConn::ExecuteCmd(pchfschar pSql)
{
	m_ErrCode = TPI_ExecSQL( m_hConn, pSql );
	if(m_ErrCode < 0 ){
		m_ErrCode = IsConnected();
		if( m_ErrCode < 0 ){
			return m_ErrCode;
		}
		m_ErrCode = TPI_ExecSQL(m_hConn,pSql);
	}
	
	return m_ErrCode;
}

hfsint CKBConn::QueryEvent(TPI_HEVENT ev)
{
	if( m_ErrCode >= _ERR_SUCCESS )
	{
		m_ErrCode = TPI_QueryEvent( m_hConn, ev );
		while( m_ErrCode == TPI_ERR_EVENTNOEND 
			|| m_ErrCode == TPI_ERR_CMDTIMEOUT 
			|| m_ErrCode == TPI_ERR_BUSY 
			|| m_ErrCode == TPI_ERR_INVALID_UID )
		{
			Sleep( 1000 );

			if( m_ErrCode == TPI_ERR_INVALID_UID )
				IsConnected();
			m_ErrCode = TPI_QueryEvent( m_hConn, ev );
		}
	}
	return m_ErrCode;
}

hfsint CKBConn::ExecuteCmdTask(pchfschar pSql)
{
	TPI_HEVENT  hTmpEvent = NULL;
	TPI_HEVENT *pEventHandle = &hTmpEvent;
	for( int i = 0; i < 5; i ++ )
	{
		m_ErrCode = TPI_ExecMgrSql( m_hConn, pSql, pEventHandle );
		if( m_ErrCode == TPI_ERR_INVALID_UID )
			IsConnected();
		else if( m_ErrCode != TPI_ERR_DOSQLERROR )
			break;
	}
	QueryEvent(*pEventHandle);
	return m_ErrCode;
}

hfsbool	CKBConn::TableExist(pchfschar pTable)
{
	if(G_DirFactory->DirExist(pTable)){
		return true;
	}
	m_ErrCode = TPI_RefreshTable(m_hConn);
	m_ErrCode = TPI_TableNameExists(m_hConn,Dcs2LocalTableName(pTable).c_str());
	if( m_ErrCode == 0 ){
		m_ErrCode = TPI_ViewNameExists(m_hConn,Dcs2LocalTableName(pTable).c_str());
	}
	return m_ErrCode == 0? false:true;
}

hfststring CKBConn::GetTablePath(pchfschar pTable)
{
	char cTablePath[MAX_PATH];
	char *p = (char*)strstr(pTable,CUtility::GetSysTempDir().c_str());
	if(p){
		strcpy(cTablePath,p+CUtility::GetSysTempDir().size());
	}
	else{
		strcpy(cTablePath,pTable);
	}
	p = strrchr(cTablePath,'\\');
	if(p){
		*p = '\0';
	}

	char szTable[MAX_PATH];
	strcpy(szTable,CUtility::GetSysWorkDir().c_str());
	strcat(szTable,cTablePath);
	strcat(szTable,"\\");
	strcat(szTable,GetFileTable(pTable).c_str());

	CUtility::DiskBase()->CreateDir(szTable);
	return szTable;
}

hfststring CKBConn::GetFileImportPath(pchfschar pTable)
{
	char cTablePath[MAX_PATH];
	char *p = (char*)strstr(pTable,CUtility::GetSysTempDir().c_str());
	if(p){
		strcpy(cTablePath,p+CUtility::GetSysTempDir().size());
	}
	else{
		strcpy(cTablePath,pTable);
	}
	p = strrchr(cTablePath,'\\');
	if(p){
		*p = '\0';
	}

	char szTable[MAX_PATH];
	strcpy(szTable,CUtility::GetSysWorkDir().c_str());
	strcat(szTable,cTablePath);
	strcat(szTable,"\\");

	return szTable;
}

hfsint CKBConn::FormatTable(SYSTEM_TABLE_FIELD_STYLE * tfs,HS_TABLE_FIELD *ptf)
{
	hfsint i = 0;
	while( tfs[i].name && *tfs[i].name && i < 32){
		strcpy( ptf[i].name, tfs[i].name );
		strcpy( ptf[i].dispName, tfs[i].name );
		ptf[i].len = tfs[i].len;
		ptf[i].type = tfs[i].type;
		ptf[i].indexkeyLen = tfs[i].indexkeyLen;
		ptf[i].indexType = (HSUINT16)tfs[i].indexType;
		i++;
	}
	return i;
}

hfsint CKBConn::CreateTable(pchfschar pTable,SYSTEM_TABLE_FIELD_STYLE * tfs)
{
	if(!pTable || !tfs){
		return _ERR_REQ_PARA;
	}
	hfststring TableName = GetFileTable(pTable).c_str();
	hfsstring  TablePath = GetTablePath(pTable);

	m_ErrCode = ImportTable(GetFileImportPath(pTable).c_str());
	if(m_ErrCode >= _ERR_SUCCESS){
		if(TableExist(pTable)){
			return m_ErrCode;
		}
	}

	HS_TABLE_HEAD  th;
	memset( &th,0,sizeof(th) );
	strcpy( th.tableName, TableName.c_str() );
	strcpy( th.tableAliasName, th.tableName);
	CUtility::GetLocalTime(th.createTime);


	HS_TABLE_FIELD tf[32];
	memset( tf, 0, sizeof(tf) );
	th.fieldNum = FormatTable(tfs,tf);

	CSmartMemoryPtr pSmart = new CSmartMemory();
	int nSize = pSmart->GetSize();
	char *pBuff = (char*)pSmart->GetPtr();

	memset( pBuff, 0, nSize );
	TPI_TABLE_INFO *pTableInfo = (TPI_TABLE_INFO *)pBuff;

	strcpy( pTableInfo->szTablePath, TablePath.c_str() );
	memcpy( &pTableInfo->TableHead, &th, sizeof(HS_TABLE_HEAD) );
	memcpy( pTableInfo->pTableField, &tf, sizeof(HS_TABLE_FIELD) * th.fieldNum );
	m_ErrCode = IsConnected();
	if(m_ErrCode >= _ERR_SUCCESS){
		CreateRoot(CUtility::Config()->GetRootDir());
		m_ErrCode = TPI_CreateTable2( m_hConn, pTableInfo, CUtility::Config()->GetRootDir().c_str() );
		if(m_ErrCode >= _ERR_SUCCESS){
			m_ErrCode = CreateView(TableName.c_str());
		}
	}
	//CUtility::Logger()->trace("CreateTable(%s) e:%d",th.tableName, m_ErrCode);

	if(m_ErrCode >= _ERR_SUCCESS){
		//G_DirFactory->CreateDir(pTable);
	}
	return m_ErrCode;
}

hfsint CKBConn::DeleteTable(pchfschar pTable,hfsbool bLogic)
{
	G_DirFactory->DeleteDir(pTable);

	m_ErrCode = IsConnected();
	CSmartMemoryPtr pSmart = new CSmartMemory();

	TPI_TABLE_INFO *t = (TPI_TABLE_INFO*)pSmart->GetPtr();
	hfsint iBufLen = pSmart->GetSize();
	hfschar cTablePath[MAX_PATH];
	m_ErrCode = TPI_GetTableInfoFromCache(m_hConn,Dcs2LocalTableName(pTable).c_str(),t,&iBufLen);
	if(m_ErrCode >= _ERR_SUCCESS){
		strcpy(cTablePath,(phfschar)pSmart->GetPtr());
		phfschar p = strrchr(cTablePath,'\\');
		if(p){
			*p='\0';
		}
	}
	if( m_ErrCode < 0 ){
		return m_ErrCode;
	}

	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = TPI_DeleteTable(m_hConn,Dcs2LocalTableName(pTable).c_str());
		if(m_ErrCode >= _ERR_SUCCESS){
			m_ErrCode = CreateView(pTable);
		}
	}
	if( bLogic ){
		if(m_ErrCode >= _ERR_SUCCESS){
			m_ErrCode = CUtility::DiskBase()->DeleteDir(cTablePath);
		}
	}
	return m_ErrCode;
}

hfsstring CKBConn::TryGetDevPath(pchfschar pTableName)
{
	hfsstring sDevDir = Dcs2LocalDirectory(pTableName);
	hfsint nDirCount = 0;
	hfsfloat nDirSize = 0;

	CUtility::DiskBase()->GetFilesInfo(sDevDir.c_str(),nDirCount,nDirSize);
	if(nDirCount <= 0 && nDirSize <= 0){
		sDevDir = Dcs2LocalSyncDirectory(pTableName);
		CUtility::DiskBase()->GetFilesInfo(sDevDir.c_str(),nDirCount,nDirSize);
	}

	if(nDirCount >= 0 && nDirSize > 0 ){
		return sDevDir;
	}

	return "";
}

hfsstring CKBConn::GetRootNameByPath(pchfschar pDevName)
{
	pchfschar p = strstr(pDevName,"T_SYS_T\\");
	if(p){
		return CUtility::Config()->GetRootDir();
	}
	return "T_SYNC_T";
}

hfsint CKBConn::ImportTable(pchfschar pTable,pchfschar pDbName)
{
	if( TableExist(pTable)){
		return _ERR_SUCCESS;
	}
	m_ErrCode = IsConnected();
	if(m_ErrCode >= _ERR_SUCCESS){
		hfsstring sDevName = TryGetDevPath(pTable);
		if(sDevName.empty()){
			//empty directory
			return _ERR_SUCCESS;
		}
		if(!pDbName){	
			CreateRoot(GetRootNameByPath(sDevName.c_str()).c_str());
			m_ErrCode = TPI_ImportTable2(m_hConn,sDevName.c_str(),
				GetRootNameByPath(sDevName.c_str()).c_str());
		}else{
			CreateRoot(pDbName);
			m_ErrCode = TPI_ImportTable2(m_hConn,Dcs2LocalDirectory(pTable).c_str(),
				pDbName);
		}
		if(m_ErrCode >= _ERR_SUCCESS){
			m_ErrCode = CreateView(pTable);
		}
	}
	return m_ErrCode;
}

hfsuint64 CKBConn::GetDirSize(pchfschar pTable)
{
	hfschar pSql[1024];
	sprintf(pSql,"select sum(FILESIZE) from %s",Dcs2LocalTableName(pTable).c_str());
	hfsint e = 0;
	if( IsConnected() >= _ERR_SUCCESS){
		TPI_HRECORDSET hSet= TPI_OpenRecordSet(m_hConn,pSql,&e);
		if( e>= _ERR_SUCCESS){
			memset(pSql,0x00,1024);
			TPI_GetFieldValue(hSet,0,pSql,1024);
		}
		TPI_CloseRecordSet(hSet);
	}
	return _atoi64(pSql);
}

void CKBConn::KTime2Systime(pchfschar ktime,phfschar systime)
{
	systime[0]='\0';
	hfschar temp[5];
	strncpy(temp,ktime,4);
	temp[4] = '\0';

	//year
	strcat(systime,temp);
	strcat(systime,"-");

	//month
	strncpy(temp,ktime+4,2);
	temp[2] = '\0';
	strcat(systime,temp);
	strcat(systime,"-");

	//day
	strncpy(temp,ktime+6,2);
	temp[2] = '\0';
	strcat(systime,temp);
	strcat(systime," ");

	//hour
	strncpy(temp,ktime+8,2);
	temp[2] = '\0';
	strcat(systime,temp);
	strcat(systime,":");

	//min
	strncpy(temp,ktime+10,2);
	temp[2] = '\0';
	strcat(systime,temp);
	strcat(systime,":");

	//sec
	strncpy(temp,ktime+12,2);
	temp[2] = '\0';
	strcat(systime,temp);
}

hfsstring CKBConn::GetDevPathX(pchfschar pTablePath)
{
	hfschar cDirPath[MAX_FILE_ID];
	strcpy(cDirPath,pTablePath);
	strcat(cDirPath,"\\");
	strcat(cDirPath,GetFileTable(pTablePath).c_str());
	return cDirPath;
}

hfsstring CKBConn::GetDevPath(pchfschar pTableName)
{
	HFS_DIR_INFO dir;
	hfsbool b = false;

	if(G_DirFactory->DirExist(pTableName)){
		if( G_DirFactory->GetDirCacheInfo(pTableName,&dir)){
			b = true;
		}
	}

	hfsint e = _ERR_SUCCESS;
	if( !b ){
		if(!TableExist(pTableName)){
			return "";
		}
		if( GetTableInfoX(pTableName,&dir) >= _ERR_SUCCESS){
			b = true;
		}
	}
	if( !b ){
		return "";
	}
	
	hfschar cDirPath[MAX_FILE_ID];
	strcpy(cDirPath,dir.tsDirName);
	strcat(cDirPath,"\\");
	strcat(cDirPath,GetFileTable(pTableName).c_str());
	return cDirPath;
}

hfsstring CKBConn::GetDevPath()
{
	if(!m_hSet){
		return "";
	}

	hfschar tname[MAX_FILE_ID];
	TPI_GetRecordTableName(m_hSet,tname);

	return GetDevPath(tname);
}

hfsint CKBConn::GetTableInfoX(pchfschar pTable,HFS_DIR_INFO *pDir)
{
	hfsint e = _ERR_SUCCESS;

	if(!TableExist(pTable)){
		return _ERR_DIR_NOT_EXIST;
	}
	memset(pDir,0x00,sizeof(HFS_DIR_INFO));

	CSmartMemoryPtr pSmart = new CSmartMemory();

	hfsint iSize = pSmart->GetSize();

	e = TPI_RefreshTableInfo(m_hConn,Dcs2LocalTableName(pTable).c_str());
	e = TPI_GetTableInfoFromCache(m_hConn,Dcs2LocalTableName(pTable).c_str(),(TPI_TABLE_INFO *)(pSmart->GetPtr()),&iSize);
	if( e >= _ERR_SUCCESS){

		TPI_TABLE_INFO *p = (TPI_TABLE_INFO*)(pSmart->GetPtr());
		pDir->btFlag = 0;
		hfschar temp[32];
		memset(temp,0x00,32);
		KTime2Systime(p->TableHead.createTime,temp);
		strcpy(pDir->tsCreateDate,temp);
		memset(temp,0x00,32);
		KTime2Systime(p->TableHead.modifyTime,temp);
		strcpy(pDir->tsModifyDate,temp);
		strcpy(pDir->tsDirName,p->szTablePath);
		pchfschar pp = strrchr(pDir->tsDirName,'\\');
		if(pp){
			*(phfschar)(pp) = '\0';
		}else{
			strcpy(pDir->tsDirName,p->TableHead.tableAliasName);
		}

		strcpy(pDir->tsFileName,Dcs2LocalTableName(pTable).c_str());
		strcpy(pDir->tsFileType,"");
		pDir->NodeID = CUtility::Config()->GetLocalNodeInfo()->NodeID;
		hfsstring DbName = GetRootNameByPath(p->szTablePath);
		if(DbName == "T_SYNC_T"){
			pDir->btFlag = 1;
		}else{
			pDir->btFlag = 0;
		}
		pDir->uiDirCount = 0;
		pDir->uiFileCount = p->TableHead.recNum;
		pDir->uiFileSize = GetDirSize(pTable);
		pDir->bIsDir=true;
	}

	G_DirFactory->CreateDir(pDir->tsFileName,pDir);

	return e;
}

hfsint CKBConn::GetTableInfo(pchfschar pTable,HFS_DIR_INFO *pDir)
{
	hfsint e = GetTableInfoX(pTable,pDir);
	strcpy(pDir->tsDirName,Local2DcsDirectory(pDir->tsDirName).c_str());
	return e;
}

hfsint CKBConn::GetAllTables(vector<string>& tables)
{
	m_ErrCode = GetDbTables(CUtility::Config()->GetRootDir().c_str(),&tables);
	if(m_ErrCode < _ERR_SUCCESS){
		return m_ErrCode;
	}
	m_ErrCode = GetDbTables("T_SYNC_T",&tables);
	if(m_ErrCode < _ERR_SUCCESS ){
		return m_ErrCode;
	}

	if(tables.empty()){
		return 0;
	}

	return (hfsint)tables.size();
}

hfsint CKBConn::CreateView(pchfschar pTable)
{
	vector<string>tables;
	vector<string>views;

	vector<string>::iterator it;
	GetAllTables(tables);
	if(tables.empty()){
		return _ERR_SUCCESS;
	}

	CSmartMemoryPtr pSmart = new CSmartMemory();
	if(!pSmart || !pSmart->GetPtr()){
		return _ERR_MEM_ALLOC;
	}

	int tc = 0;
	string ctables;

	while(tc < (hfsint)tables.size()){
		ctables = CUtility::Str()->ListToString(tables,",",tc,tc + 500);
		if(ctables.empty()){
			break;
		}

		char subview[256];
		sprintf(subview,"%s_%03d",GetFileView().c_str(),(tc/500+1));
		TPI_DeleteView(m_hConn,subview);

		sprintf((phfschar)pSmart->GetPtr(),"SELECT * FROM %s WHERE",ctables.c_str());
		m_ErrCode = TPI_CreateView(m_hConn, subview,(phfschar)pSmart->GetPtr(),false);
		
		if(m_ErrCode < _ERR_SUCCESS){
			return m_ErrCode;
		}

		views.push_back(subview);

		tc += 500;
	}

	ctables = CUtility::Str()->ListToString(views,",",0,views.size());

	sprintf((phfschar)pSmart->GetPtr(),"SELECT * FROM %s WHERE",ctables.c_str());
	TPI_DeleteView(m_hConn,GetFileView().c_str());
	m_ErrCode = TPI_CreateView(m_hConn, GetFileView().c_str(),(phfschar)pSmart->GetPtr(),false);
	
	return m_ErrCode;
}

hfsstring CKBConn::GetFindTableX(pchfschar pFullName,bool& bFuzzy)
{
	bFuzzy = true;
	vector<string> token;
	hfschar	cDirName[MAX_FILE_ID];
	cDirName[0] = '\0';
	strcpy(cDirName,pFullName);
	if( cDirName[strlen(pFullName)-1] == '\\' || (strlen(cDirName) > 2 &&  cDirName[strlen(pFullName)-2] == '\\')){
		cDirName[strlen(cDirName)-1] = '\0';
		CUtility::Str()->CStrToStrings(cDirName,token,'\\');
		if( (hfsint)token.size() > 0){
			strcpy(cDirName,token[(hfsint)token.size()-1].c_str());
			bFuzzy = false;
		}
		else{ 
			strcpy(cDirName,"CNKI_DATAVIEW");
		}

	}else{
		CUtility::Str()->CStrToStrings(pFullName,token,'\\');
		if( (hfsint)token.size() > 1){
			strcpy(cDirName,token[(hfsint)token.size()-2].c_str());
		}else{
			cDirName[0] = '\0';
		}
	}
	return cDirName;
}

hfsstring CKBConn::GetFindTable(pchfschar pFullName)
{
	vector<string> token;
	hfschar	cDirName[MAX_FILE_ID];
	cDirName[0] = '\0';
	strcpy(cDirName,pFullName);
	if( cDirName[strlen(pFullName)-1] == '\\'){
		cDirName[strlen(cDirName)-1] = '\0';
		CUtility::Str()->CStrToStrings(cDirName,token,'\\');
		if( (hfsint)token.size() > 0){
			strcpy(cDirName,token[(hfsint)token.size()-1].c_str());
		}
		else{ 
			strcpy(cDirName,"CNKI_DATAVIEW");
		}

	}else{
		CUtility::Str()->CStrToStrings(pFullName,token,'\\');
		if( (hfsint)token.size() > 1){
			strcpy(cDirName,token[(hfsint)token.size()-2].c_str());
		}
		else{ 
			strcpy(cDirName,"CNKI_DATAVIEW");
		}
	}
	return cDirName;
}

hfststring CKBConn::GetFileTable(pchfschar pFullName)
{
	vector<string> token;
	hfschar	cDirName[MAX_FILE_ID];
	cDirName[0] = '\0';
	strcpy(cDirName,pFullName);
	if( cDirName[strlen(pFullName)-1] == '\\'){
		cDirName[strlen(cDirName)-1] = '\0';
		CUtility::Str()->CStrToStrings(cDirName,token,'\\');
		if( (hfsint)token.size() > 0){
			strcpy(cDirName,token[(hfsint)token.size()-1].c_str());
		}
		else{ 
			strcpy(cDirName,pFullName);
		}

	}else{
		CUtility::Str()->CStrToStrings(pFullName,token,'\\');
		if( (hfsint)token.size() > 1){
			strcpy(cDirName,token[(hfsint)token.size()-2].c_str());
		}
		else{ 
			strcpy(cDirName,pFullName);
		}
	}
	return cDirName;
}

hfststring CKBConn::GetFileView()
{
	return CUtility::Config()->GetRootDir()+"VIEW";
}

hfststring CKBConn::GetFileName(pchfschar pFullName)
{
	vector<string> token;
	hfschar cShortName[MAX_FILE_ID];
	cShortName[0] = '\0';

	if(pFullName[strlen(pFullName)-1] == '\\'){
		return "*";
	}
	CUtility::Str()->CStrToStrings(pFullName,token,'\\');
	if( (hfsint)token.size() > 0){
		strcpy(cShortName,token[(hfsint)token.size()-1].c_str());
	}
	return cShortName;
}

hfsstring CKBConn::GetFileExtName(pchfschar pFullName)
{
	phfschar p = (phfschar)strrchr(pFullName,'.');
	if(p && p+1 && *(p+1) != '\0'){
		return p+1;
	}
	return "";
}

hfststring CKBConn::GetFileNameX(pchfschar pFullName)
{
	hfsstring sShorName=GetFileName(pFullName);
	hfschar cShortName[MAX_FILE_ID];
	strcpy(cShortName,sShorName.c_str());
	phfschar p = strrchr(cShortName,'.');
	if(p){
		*p='\0';
	}
	return cShortName;
}

hfsbool	CKBConn::IsExist(pchfschar pFullName)
{
	m_ErrCode = IsConnected();

	if(m_ErrCode >= _ERR_SUCCESS){
		hfschar pSql[1024];
		hfsstring extName = GetFileExtName(pFullName);
		if(extName.empty()){
			sprintf(pSql,"SELECT COUNT(*) FROM %s WHERE FILENAME='%s'",
				GetFileTable(pFullName).c_str(),GetFileNameX(pFullName).c_str());
		}else{
			sprintf(pSql,"SELECT COUNT(*) FROM %s WHERE FILENAME='%s' AND FILETYPE='%s'",
				GetFileTable(pFullName).c_str(),GetFileNameX(pFullName).c_str(),extName.c_str());
		}
		TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
		if(m_ErrCode >= _ERR_SUCCESS && hSet){
			hfsint c = TPI_GetRecordSetCount(hSet);
			TPI_CloseRecordSet(hSet);
			return c> 0 ?true:false;
		}
	}
	return false;
}

hfsstring CKBConn::GetOpenString(pchfschar pFullName,pchfschar pTables)
{
	CSmartMemoryPtr pSamrt = new CSmartMemory();
	if(!pSamrt){
		return "";
	}
	phfschar cSql = (phfschar)pSamrt->GetPtr();

	hfsstring sFileName = GetFileNameX(pFullName);

	if(!pTables){
		sprintf(cSql,"SELECT * FROM %s WHERE FILENAME='%s'",
			GetFindTable(pFullName).c_str(),sFileName.c_str());	

	}else{
		sprintf(cSql,"SELECT * FROM %s WHERE FILENAME='%s'",
			pTables,sFileName.c_str());	
	}
	return cSql;
}

void CKBConn::Trim(hfschar* str,hfschar ch)
{
	hfsint i;
	hfsint iSize = (hfsint)strlen(str);
	for(i=iSize; i>0; i--)
	{
		if(str[i-1] == ch)
			str[i-1] = 0;
		else
			break;
	}
	iSize = (hfsint)strlen(str);
	for(i=0; (hfsint)i<iSize; i++)
	{
		if(str[i] != ch)
			break;
	}
	if(i)
		strcpy(str, str+i);
}
hfsstring CKBConn::GetFindStringWithOutKey(pchfschar pFileName)
{
	hfschar cSql[1024];
	sprintf(cSql,"SELECT * FROM %s WHERE",
			GetFindTable(pFileName).c_str());

	return cSql;
}

hfsstring CKBConn::GetFindString(pchfschar pFullName)
{
	hfschar cSql[1024];
	hfschar cFindKey[MAX_FILE_ID];
	
	hfsstring sExtName  = GetFileExtName(pFullName);
	hfsstring sFileName = GetFileNameX(pFullName);

	if(!sFileName.empty()){
		if(!stricmp("*",sFileName.c_str())){
			sFileName = "'*'";
		}
		else{
			strcpy(cFindKey,sFileName.c_str());
			if(strstr(sFileName.c_str(),"*")){
				Trim(cFindKey,'*');
			}
			sFileName = "?" + (hfsstring)cFindKey;
		}
	}else{
		sFileName = "'*'";
	}
	if(!sExtName.empty()){
		if(!stricmp("*",sExtName.c_str())){
			sExtName.clear();
		}
		else{
			strcpy(cFindKey,sExtName.c_str());
			if(strstr(sExtName.c_str(),"*")){
				Trim(cFindKey,'*');
			}
			sExtName = "?" + (hfsstring)cFindKey;
		}
	}
	bool bFuzzy = true;
	hfsstring tableName = GetFindTableX(pFullName,bFuzzy);
	if(sExtName.empty()){
		if(!tableName.empty()){
			if(bFuzzy){
				sprintf(cSql,"SELECT * FROM CNKI_DATAVIEW WHERE FILENAME=%s AND DIRNAME = ?%s",
					sFileName.c_str(),tableName.c_str());
			}else{
				sprintf(cSql,"SELECT * FROM %s WHERE FILENAME=%s AND DIRNAME = ?%s",
					tableName.c_str(),sFileName.c_str(),tableName.c_str());
			}
		}else{
			sprintf(cSql,"SELECT * FROM CNKI_DATAVIEW WHERE FILENAME=%s",
				sFileName.c_str());
		}

	}else{
		if(!tableName.empty()){
			sprintf(cSql,"SELECT * FROM CNKI_DATAVIEW WHERE FILENAME=%s AND FILETYPE=%s AND DIRNAME = ?%s",
				sFileName.c_str(),sExtName.c_str(),tableName.c_str());
		}else{
			sprintf(cSql,"SELECT * FROM CNKI_DATAVIEW WHERE FILENAME=%s AND FILETYPE=%s",
				sFileName.c_str(),sExtName.c_str());
		}
	}

	return cSql;
}

hfsint CKBConn::OpenTableFile(pchfschar pFullName)
{
	vector<string>tables;
	GetAllTables(tables);
	if(tables.empty()){
		return _ERR_FILE_NOEXIST;
	}

	int tc = 0;
	string ctables;
	hfsint cc = 0;

	while(tc < (hfsint)tables.size()){

		ctables = CUtility::Str()->ListToString(tables,",",tc,tc + 500);
		if(ctables.empty()){
			break;
		}
		hfsstring	sSql = GetOpenString(pFullName,ctables.c_str());
		m_hSet = TPI_OpenRecordSet(m_hConn,sSql.c_str(),&m_ErrCode);
		if(m_ErrCode >= 0 && m_hSet){
			cc = TPI_GetRecordSetCount(m_hSet);
			if(cc > 0 ){
				break;
			}
		}
		if(m_hSet){
			TPI_CloseRecordSet(m_hSet);
		}
		tc += 500;
	}

	return cc;
}

hfsint CKBConn::OpenFile(pchfschar pFullName)
{
	hfsstring	sSql = GetOpenString(pFullName);
	m_hSet = TPI_OpenRecordSet(m_hConn,sSql.c_str(),&m_ErrCode);

	if(m_ErrCode < _ERR_SUCCESS){
		return m_ErrCode;
	}

	if(!m_hSet){
		return _ERR_FILE_NOEXIST;
	}

	hfsint cc = TPI_GetRecordSetCount(m_hSet);
		
	if(!cc){
		TPI_CloseRecordSet(m_hSet);
		/*if(OpenTableFile(pFullName) <= 0 || !m_hSet){
			return _ERR_FILE_NOEXIST;
		}
		*/
		return _ERR_FILE_NOEXIST;
	}

	hfsstring sExtName = GetFileExtName(pFullName);
	hfschar tname[MAX_FILE_ID];
	strcpy(tname,sExtName.c_str());
	_strupr(tname);

	bool bFind = true;

	if(!sExtName.empty() && sExtName != "*"){
			
		bFind = false;
		while( !TPI_IsEOF(m_hSet)){
			char t[MAX_FILE_EXT];
			m_ErrCode = TPI_GetFieldValueByName(m_hSet,"FILETYPE",t,MAX_FILE_EXT);	
			if(m_ErrCode < 0 ){
				TPI_CloseRecordSet(m_hSet);
				//CUtility::Logger()->d("error.[%d]",m_ErrCode);
				break;
			}
			_strupr(t);

			if(strstr(tname,t)){
				bFind = true;
				break;
			}
			TPI_MoveNext(m_hSet);
		}
	}

	if(!bFind){
		TPI_CloseRecordSet(m_hSet);
		m_ErrCode = _ERR_FILE_NOEXIST;;
	}
	return m_ErrCode;
}
hfsint CKBConn::IndexDir(pchfschar pFullName)
{
	char cSql[1024];
	sprintf(cSql, "INDEX %s ALL",GetFindTable(pFullName).c_str());
	m_ErrCode = ExecuteCmdTask(cSql);
	return m_ErrCode;
}

hfsint CKBConn::OpenDir(pchfschar pFullName)
{
	hfsstring	sSql = GetFindStringWithOutKey(pFullName);
	m_hSet = TPI_OpenRecordSet(m_hConn,sSql.c_str(),&m_ErrCode);

	if(m_hSet){
		hfsint cc = TPI_GetRecordSetCount(m_hSet);
		if(m_ErrCode < _ERR_SUCCESS){
			TPI_CloseRecordSet(m_hSet);
			return m_ErrCode;
		}
		if(!cc)
		{
			TPI_CloseRecordSet(m_hSet);
			return _ERR_FILE_OPEN;
		}
		int irand = rand() % cc;

		Move(irand);
	}
	return m_ErrCode;
}

hfsint CKBConn::FindFile(pchfschar pFullName)
{
	hfsstring	sSql = GetFindString(pFullName);
	m_hSet = TPI_OpenRecordSet(m_hConn,sSql.c_str(),&m_ErrCode);

	if(m_hSet){
		hfsint cc = TPI_GetRecordSetCount(m_hSet);
		if(m_ErrCode < _ERR_SUCCESS){
			TPI_CloseRecordSet(m_hSet);
			return m_ErrCode;
		}
		if(!cc)
		{
			TPI_CloseRecordSet(m_hSet);
			return _ERR_FILE_OPEN;
		}
		MoveFirst();
	}
	return m_ErrCode;
}

hfsbool CKBConn::MoveFirst()
{
	return TPI_MoveFirst(m_hSet);
}

hfsbool CKBConn::MoveLast()
{
	return TPI_MoveLast(m_hSet);
}

hfsbool CKBConn::MoveNext()
{
	return TPI_MoveNext(m_hSet);
}

hfsbool	CKBConn::MovePrev()
{
	return TPI_MovePrev(m_hSet);
}

hfsbool CKBConn::Move(hfsint pos)
{
	MoveFirst();
	int p = 0;
	hfsbool b=true;
	while(p < pos && b){
		hfsbool b = MoveNext();
		p++;
	}
	return true;
}
hfsuint64 CKBConn::GetFileCount()
{
	hfslong l= TPI_GetRecordSetCount(m_hSet);
	if(l < _ERR_SUCCESS ){
		return 0;
	}
	return l;
}

hfsint CKBConn::DeleteFileKbase(pchfschar pFullName)
{
	hfschar pSql[1024];

	hfsstring cTable = Dcs2LocalTableName(pFullName);
	hfsstring extName = GetFileExtName(pFullName);
	if(extName.empty()){
		sprintf(pSql,"DELETE FROM %s WHERE FILENAME='%s'",
			GetFileTable(pFullName).c_str(),GetFileNameX(pFullName).c_str());
	}else{
		sprintf(pSql,"DELETE FROM %s WHERE FILENAME='%s' AND FILETYPE='%s'",
			GetFileTable(pFullName).c_str(),GetFileNameX(pFullName).c_str(),extName.c_str());
	}

	m_ErrCode = IsConnected();

	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = ExecuteCmd(pSql);
	}

	//未同步的数据误报出错信息
	if(m_ErrCode == TPI_ERR_DOSQLERROR){
		m_ErrCode = _ERR_SUCCESS;
	}
	return m_ErrCode;
}

hfsint CKBConn::GetDirFiles(hfsint pos,hfsint size,vector<hfsstring>* files)
{
	if( !Move(pos)){
		return 0;
	}
	hfsint i = 0; 
	while( i < size ){
		hfschar buf[MAX_FILE_ID];
		buf[0] = '\0';
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"FILENAME",buf,MAX_FILE_ID);
		if( m_ErrCode < _ERR_SUCCESS){
			break;
		}
		if(buf[0] != '\0'){
			strupr(buf);
			files->push_back(buf);
		}

		MoveNext();
		i++;
	}

	return (hfsint)files->size();
}

hfsint CKBConn::GetFileInfo(HFS_FILE_INFO *fd)
{
	HFS_DIR_INFO dir;
	if(!m_hSet){
		return _ERR_SUCCESS;
	}

	if( TPI_GetRecordTableName(m_hSet,fd->tsDirName) < 0 ){
		return _ERR_SUCCESS;
	}

	if( !G_DirFactory->GetDirCacheInfo(fd->tsDirName,&dir)){
		m_ErrCode = GetTableInfoX(fd->tsDirName,&dir);
	}
	strcpy(dir.tsDirName,Local2DcsDirectory(dir.tsDirName).c_str());

	if(m_ErrCode < _ERR_SUCCESS){
		return m_ErrCode;
	}
	fd->NodeID = dir.NodeID;
	strcpy(fd->tsDirName,dir.tsDirName);
	fd->btFlag = dir.btFlag;

	hfschar buf[64];
	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"FILENAME",fd->tsFileName,MAX_FILE_ID);
	}
	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"CREATEDATE",fd->tsCreateDate,20);
	}
	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"MODIFYDATE",fd->tsModifyDate,20);
	}
	if(m_ErrCode >= _ERR_SUCCESS){
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"FILETYPE",fd->tsFileType,MAX_FILE_EXT);
	}
	if(m_ErrCode >= _ERR_SUCCESS){
		memset(buf,0x00,64);
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"MD5",buf,63);
		strcpy(fd->MD5,buf);
	}

	if(m_ErrCode >= _ERR_SUCCESS){
		memset(buf,0x00,64);
		m_ErrCode = TPI_GetFieldValueByName(m_hSet,"FILESIZE",buf,63);
		fd->uiFileSize=_atoi64(buf);
	}
	fd->uiDirCount = fd->uiFileCount=0;

	memset(buf,0x00,64);
	m_ErrCode = TPI_GetFieldValueByName(m_hSet,"NODEID",buf,63);
	fd->NodeID = atoi(buf);

#ifdef HFS_KFS
	fd->col = 7;
#else
	memset(buf,0x00,64);
	m_ErrCode = TPI_GetFieldValueByName(m_hSet,"DATA",buf,63);
	fd->pos.ulPos = _atoi64(buf);
#endif
	fd->bIsDir=false;
	return m_ErrCode;
}

hfsint CKBConn::WriteFile(hfststring pFullName)
{
	//G_Guard Lock(m_Lock);
	hfsstring TableName = GetFileTable(pFullName.c_str());
	
	CRecFilePtr r = new CRecFile(pFullName);
	if( r->Write() < _ERR_SUCCESS){
		return _ERR_FILE_OPEN;
	}
	hfschar pSql[1024];
	sprintf( pSql, "BULKLOAD TABLE %s '%s'", 
		GetFileTable(pFullName.c_str()).c_str(), 
		r->GetRecName().c_str());

	m_ErrCode = ExecuteCmd(pSql);

	return m_ErrCode;
}

hfsint CKBConn::WriteFile(hfststring pFullName,hfsuint64 pos)
{
	G_Guard Lock(m_Lock);
	hfsstring TableName = GetFileTable(pFullName.c_str());
	
	CRecFilePtr r = new CRecFile(pFullName);
	hfsstring sql = r->GetSQL(TableName,pos);
	//CUtility::Logger()->trace(sql.c_str());
	m_ErrCode = ExecuteCmd(sql.c_str());

	return m_ErrCode;

#ifdef LOAD_INIT

	hfsstring TableName = GetFileTable(pFullName.c_str());
	
	static map<string,CFileDriverPtr> recmap;
	map<string,CFileDriverPtr>::iterator it;
	it = recmap.find(TableName);
	if(it == recmap.end()){
		string path = "d:\\"+TableName+".txt";
		CFileDriverPtr fp = new CFileDriver(path);
		fp->fopen(path.c_str(),"w");
		recmap.insert(make_pair(TableName,fp));
		it = recmap.find(TableName);
	}

	if(it == recmap.end()){
		return _ERR_FILE_OPEN;
	}
	CRecFilePtr r = new CRecFile(pFullName);
	hfsstring rectext = r->RecText(pos);
	if(rectext.empty()){
		return _ERR_FILE_OPEN;
	}

	it->second->fwrite(rectext.c_str(),1,rectext.size());
	it->second->fflush();
	
	return m_ErrCode;
#endif

}

hfsint CKBConn::ReadFile(phfschar pBuffer,hfssize iOffset,hfsint iByte)
{
	G_Guard Lock(m_Lock);
	if(!m_hSet){
		return _ERR_INVALID_HANDLE;
	}
	hfsint e = TPI_GetBLob(m_hSet,7,0,(hfsint)iOffset,pBuffer,iByte);
	if( e < _ERR_SUCCESS){
		m_ErrCode = IsConnected();
		e = TPI_GetBLob(m_hSet,7,0,(hfsint)iOffset,pBuffer,iByte);
	}
	return e;
}

hfsbool CKBConn::IsViewExist(hfsstring pView)
{
	m_ErrCode = IsConnected();
	if(m_ErrCode < _ERR_SUCCESS){
		return false;
	}
	return TPI_ViewNameExists(m_hConn,pView.c_str())==1?true:false;
}

hfsbool CKBConn::IsRootExist(hfsstring pRoot)
{
	m_ErrCode = IsConnected();
	if(m_ErrCode < _ERR_SUCCESS){
		return false;
	}
	hfsint iSize = 256;
	hfschar cDbName[256];
	memset( cDbName, 0, iSize );
	int nCount = TPI_GetDatabaseCount( m_hConn );
	for( int i= 0; i < nCount; i ++ )
	{
		TPI_GetDatabaseName(m_hConn, i, cDbName, iSize );
		if( !stricmp( cDbName,pRoot.c_str() ) )
			return true;

		iSize = 256;
		memset( cDbName, 0, iSize );
	}
	return false;
}

hfsint	CKBConn::GetDbTables(pchfschar dbName,vector<string>* tables)
{
	m_ErrCode = IsConnected();
	CSmartMemoryPtr pSmart = new CSmartMemory();

	hfsint iSize = pSmart->GetSize();
	
	m_ErrCode = TPI_GetTablesListInDB(m_hConn,dbName,(phfschar)pSmart->GetPtr(),&iSize);
	if(m_ErrCode >= _ERR_SUCCESS && iSize > 0 ){
		if(*(phfschar)pSmart->GetPtr() == '\0'){
			return 0;
		}
		CUtility::Str()->CStrToStrings((phfschar)pSmart->GetPtr(),*tables,',');
		m_ErrCode = (hfsint)tables->size();
	}
	return m_ErrCode;
}

hfsint CKBConn::CreateRoot(hfsstring pRoot)
{
	m_ErrCode = IsConnected();
	if(!IsRootExist(pRoot))
	{
		m_ErrCode = TPI_CreateDataBase(m_hConn,pRoot.c_str());
	}

	return m_ErrCode;
}

hfststring CKBConn::Local2DcsDirectory(pchfschar pDirName)
{
	return CUtility::DiskName2SysName(pDirName);
}

hfststring CKBConn::Dcs2LocalTableName(pchfschar pDirName,hfsbool bView /* = false */)
{
	hfschar	cDirName[MAX_FILE_ID];
	hfschar ct[MAX_FILE_ID];

	phfschar p = (phfschar)strrchr(pDirName,'.');
	if(p){
		return GetFileTable(pDirName);
	}
	strcpy(ct,pDirName);
	if(ct[strlen(ct)-1] == '\\'){
		ct[strlen(ct)-1] = '\0';
	}
	p = (phfschar)strrchr(ct,'\\');
	if(!p){
		strcpy(cDirName,ct);
	}else{
		strcpy(cDirName,++p);
	}
	return cDirName;
}

hfststring CKBConn::Dcs2LocalSyncDirectory(pchfschar pDirName,hfsbool bView)
{
	hfschar cLocalName[MAX_FILE_ID];
	hfschar cTempName[MAX_FILE_ID];

	strcpy(cLocalName,pDirName);
	strcpy(cTempName,CUtility::GetSysSyncDir().c_str());

	strupr(cLocalName);
	strupr(cTempName);

	pchfschar p = strstr(cLocalName,cTempName);
	if(p){
		return cLocalName;
	}
	else{
		strcpy(cLocalName,CUtility::GetSysSyncDir().c_str());
		strcat(cLocalName,pDirName);
	}
	return cLocalName;
}

hfststring CKBConn::Dcs2LocalDirectory(pchfschar pDirName,hfsbool bView)
{
	hfschar cLocalName[MAX_FILE_ID];
	hfschar cTempName[MAX_FILE_ID];
	
	strcpy(cLocalName,pDirName);
	strcpy(cTempName,CUtility::GetSysWorkDir().c_str());

	strupr(cLocalName);
	strupr(cTempName);

	pchfschar p = strstr(cLocalName,cTempName);
	if(p){
		return cLocalName;
	}
	else{
		strcpy(cLocalName,CUtility::GetSysWorkDir().c_str());
		strcat(cLocalName,pDirName);
	}
	return cLocalName;
}

hfsint CKBConn::DisableTable(pchfschar pTable)
{
	hfsstring cTable = Dcs2LocalTableName(pTable);
	CLockObjectXPtr lock =  CUtility::Lock()->GetXLock(cTable);
	if( !lock){
		return _ERR_GET_LOCK;
	}
	
	G_DirFactory->DeleteDir(pTable);

	return DeleteTable(pTable);
}

hfsint	CKBConn::EnableTable(pchfschar pTable)
{
	hfsstring cTable = Dcs2LocalTableName(pTable);
	CUtility::Lock()->UnLockObject(cTable);

	int tCount = ImportTable(pTable);
	if( tCount > 0 ){
		G_DirFactory->CreateDir(pTable);
	}

	return _ERR_SUCCESS;
}

hfsbool CKBConn::IsTableLock(pchfschar pTable)
{
	hfsstring cTable = Dcs2LocalTableName(pTable);
	return CUtility::Lock()->IsLock(cTable);
}

hfsbool CKBConn::IsFindDir()
{
	if(m_hSet){
		return false;
	}

	return true;
}

hfsint CKBConn::ChangeNodeID(HFS_DIR_INFO* dir)
{
	char sql[1024];
	sprintf(sql, "UPDATE %s SET NODEID = '%d'",dir->tsFileName,CUtility::Config()->GetLocalNodeInfo()->NodeID);
	hfsint e = ExecuteCmd(sql);

	return e;
}



hfsbool	CKBConn::ParserFormat(CMarkup &xParser,vector<GROUPMATCH*> &groups)
{
	xParser.ResetMainPos();

	if( xParser.FindElem("literatureFormat")){
		xParser.IntoElem();
			while(xParser.FindElem("group")){
				GROUPMATCH* gm = new GROUPMATCH();
				memset(gm,0x00,sizeof(GROUPMATCH));
				strcpy(gm->id,ParserID(xParser).c_str());
				if( LoadGroup(xParser,gm)){
					AddGroup(gm,groups);
				}else{
					return false;
				}
			}
		xParser.OutOfElem();
	}else{
		return false;
	}
	return true;
}

hfsbool CKBConn::LoadGroup(CMarkup &xParser, GROUPMATCH *gm)
{
	string strTemp;

	xParser.IntoElem();

	strcpy(gm->literatureType,ParserName(xParser).c_str());
	strcpy(gm->fileSaveMode,ParserFileSaveMode(xParser).c_str());
	strcpy(gm->baseUrl,ParserBaseUrl(xParser).c_str());
	strcpy(gm->preWord,ParserPreWord(xParser).c_str());
	strcpy(gm->postWord,ParserPostWord(xParser).c_str());

	while(xParser.FindElem("dataBase")){
		DATABASEMATCH *dm = new DATABASEMATCH();
		memset(dm,0x00,sizeof(DATABASEMATCH));
		if( LoadDataBase(xParser,dm)){
			AddDataBase(gm,dm);
		}else{
			return false;
		}
	}


	xParser.OutOfElem();
	return true;
}


void CKBConn::AddGroup(GROUPMATCH *group,vector<GROUPMATCH*> &groups)
{
	GROUPMATCH* pGroup = new GROUPMATCH;
	memcpy(pGroup,group,sizeof(GROUPMATCH));
	groups.push_back(pGroup);
}

string CKBConn::ParserName(CMarkup &xParser)
{
	if(xParser.FindElem("name")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserPreWord(CMarkup &xParser)
{
	if(xParser.FindElem("preWord")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserPostWord(CMarkup &xParser)
{
	if(xParser.FindElem("postWord")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserFileSaveMode(CMarkup &xParser)
{
	if(xParser.FindElem("FileSaveMode")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserBaseUrl(CMarkup &xParser)
{
	if(xParser.FindElem("baseUrl")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserTableID(CMarkup &xParser)
{
	if(xParser.FindElem("tableID")){
		return xParser.GetData();
	}
	return "";
}

string CKBConn::ParserShowStyle(CMarkup &xParser)
{
	if(xParser.FindElem("showstyle")){
		return xParser.GetData();
	}
	return "";
}


void CKBConn::AddDataBase(GROUPMATCH *gm,DATABASEMATCH *database)
{
	DATABASEMATCH* pDataBase = new DATABASEMATCH;
	memcpy(pDataBase,database,sizeof(DATABASEMATCH));
	gm->databaseList.push_back(pDataBase);
}

hfsbool CKBConn::LoadDataBase(CMarkup &xParser, DATABASEMATCH *dm)
{
	string strTemp;

	xParser.IntoElem();

	strTemp = ParserTableID(xParser);
	CUtility::Str()->CStrToStrings(strTemp.c_str(),dm->tableIdList,',');
	strcpy(dm->showstyle,ParserShowStyle(xParser).c_str());

	while(xParser.FindElem("fieldMatch")){
		MATERSUBMATCH *msm = new MATERSUBMATCH;
		memset(msm,0x00,sizeof(MATERSUBMATCH));
		if( LoadMaterSubMatch(xParser,msm)){
			AddMaterSubMatch(dm,msm);
		}else{
			return false;
		}
	}

	xParser.OutOfElem();
	return true;
}


hfsbool CKBConn::LoadMaterSubMatch(CMarkup &xParser, MATERSUBMATCH *msm)
{
	string strTemp;

	xParser.IntoElem();

	strcpy(msm->haveSubfield,ParserHaveSubField(xParser).c_str());

	strTemp = msm->haveSubfield;
	if (strTemp.compare("0") == 0)
	{
		strcpy(msm->masterField.sourceField,ParserSource(xParser).c_str());
		strcpy(msm->masterField.destField,ParserDest(xParser).c_str());
		strcpy(msm->masterField.opType,ParserType(xParser).c_str());
	}
	else
	{
		if (xParser.FindElem("masterField"))
		{
			xParser.IntoElem();
			strcpy(msm->subField.masterName,ParserName(xParser).c_str());
			while(xParser.FindElem("subCode")){
				MATCH *subPair = new MATCH;
				memset(subPair,0x00,sizeof(MATCH));
				if( LoadMatch(xParser,subPair)){
					AddMatch(msm,subPair);
				}else{
					return false;
				}
			}

			xParser.OutOfElem();
		}
	}
	xParser.OutOfElem();
	return true;
}

void CKBConn::AddMaterSubMatch(DATABASEMATCH* dm,MATERSUBMATCH *msm)
{
	MATERSUBMATCH* pMaterSubMatch = new MATERSUBMATCH;
	memset(pMaterSubMatch,0,sizeof(MATERSUBMATCH));
	memcpy(pMaterSubMatch,msm,sizeof(MATERSUBMATCH));
	dm->fieldMatchList.push_back(pMaterSubMatch);
}

string	CKBConn::ParserHaveSubField(CMarkup &xParser)
{
	if(xParser.FindElem("haveSubfield")){
		return xParser.GetData();
	}
	return "";
}

string	CKBConn::ParserSource(CMarkup &xParser)
{
	if(xParser.FindElem("source")){
		return xParser.GetData();
	}
	return "";
}

string	CKBConn::ParserDest(CMarkup &xParser)
{
	if(xParser.FindElem("dest")){
		return xParser.GetData();
	}
	return "";
}

string	CKBConn::ParserType(CMarkup &xParser)
{
	if(xParser.FindElem("type")){
		return xParser.GetData();
	}
	return "";
}

string	CKBConn::ParserID(CMarkup &xParser)
{
	return  xParser.GetAttrib("id");
}


hfsbool CKBConn::LoadMatch(CMarkup &xParser, MATCH *match)
{
	xParser.IntoElem();

	strcpy(match->sourceField,ParserSource(xParser).c_str());
	strcpy(match->destField, ParserDest(xParser).c_str());
	strcpy(match->opType, ParserType(xParser).c_str());

	xParser.OutOfElem();
	return true;
}

void CKBConn::AddMatch(MATERSUBMATCH* msm,MATCH *subPair)
{
	MATCH* pMatch = new MATCH;
	memset(pMatch,0,sizeof(MATCH));
	memcpy(pMatch,subPair,sizeof(MATCH));
	msm->subField.subCode.push_back(pMatch);
}


DATABASEMATCH  *CKBConn::GroupMatchCotainDBCode(GROUPMATCH *group, string tableName)
{
    DATABASEMATCH *dm = 0;
	vector<DATABASEMATCH*>::iterator it;
	hfsint iCount = group->databaseList.size();
	hfsbool bBreak = false;
	for (hfsint i = 0;i < (int)group->databaseList.size();++i)
	{
		dm = group->databaseList[i];
		for (hfsint j = 0;j < (int)dm->tableIdList.size();++j)
		{
			hfsint iLength = dm->tableIdList[j].length();
			string strTemp = tableName.substr(0,iLength);
			if (dm->tableIdList[j].compare(strTemp) == 0)
			{
				bBreak = true;
				break;
			}
		}
		if (bBreak)
		{
			break;
		}
	}
    return dm;
}

hfsint	CKBConn::WriteRecord(DOWN_STATE * taskdata)
{
	int nType = taskdata->nStatType;
	int nRet = 0;
	switch(nType)    
	{
		case REF_RUNQIKAONLY:	//高被引期刊
			//nRet = RunQIKA(taskdata);
			break;

		case REF_RUNAUTHONLY:	//高被引作者
			//nRet = RunAUTH(taskdata);
			break;

		case REF_RUNHOSPONLY:	//高被引医院
			//nRet = RunHOSP(taskdata);
			break;
            
		case REF_RUNUNIVONLY:	//高被引院校
			//nRet = RunUNIV(taskdata);
			break;

		case REF_RUNZTCSONLY:	//高被引专题
			//nRet = RunZTCS(taskdata);
			break;
            
		case REF_RUNDOCUONLY:	//高被引文献
			//nRet = RunDOCU(taskdata);
			break;

		case REF_RUNDICTONLY:	//词典
			//nRet = RunDICT(taskdata);
			break;

		case REF_RUNSTATONLY:	//统计各专题数
			//nRet = RunSTAT(taskdata);
			break;

		case REF_RUNDOWNLOAD:	//统计下载数
			//nRet = RunDOWNLOAD(taskdata);
			break;

		case REF_COLUMNHIERA:	//期刊栏目层次统计
			//nRet = RunCOLUHIER(taskdata);
			break;

		case REF_TEXTGET:	//得到text中的数据
			nRet = RunTextGet(taskdata);
			break;

		case REF_CAPJGET:	//得到CAPJLAST表中的优先出版=Y+A的数据
			{
				nRet = RunCapjGet(taskdata);
				nRet = RunCapjGetSf(taskdata);
				break;
			}
		case REF_CAPJGETLOOP:	//得到CAPJDAY表中的优先出版=Y+A的数据
			{
				nRet = RunCapjGetLoop(taskdata);
				nRet = RunCapjGetLoopSf(taskdata);
				break;
			}

		default:
			break;
	}
	return 0;
}


hfsstring	CKBConn::GetFieldValueByName(TPI_HRECORDSET &hSet,string strFieldName)
{
	hfsstring strValue = "";
	char t[MAX_QUERY]={0};
	memset(t,0,sizeof(t));
	m_ErrCode = TPI_GetFieldValueByName(hSet,strFieldName.c_str(),t,MAX_QUERY);	
	if(m_ErrCode < 0 ){
	}
	else
	{
		strValue = t;
	}
	return strValue;
}

hfsbool		CKBConn::LoadZJCLS(vector<ST_ZJCLS*> &m_ZJCls)
{
	if( !m_ZJCls.empty() )
		return 1;

	int m_ErrCode = IsConnected();

	char szSql[128] = {0};
	sprintf( szSql, "select SYS_FLD_CLASS_CODE,SYS_FLD_CLASS_NAME from  %s where SYS_FLD_CLASS_GRADE=2", "zjcls" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	m_ErrCode  = TPI_GetRecordSetCount( hSet );

	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < m_ErrCode; i ++ )
	{
		ST_ZJCLS *item = new ST_ZJCLS;
		memset( item, 0, sizeof(ST_ZJCLS) );

		nLen = (int)sizeof(item->szCode);
		TPI_GetFieldValue( hSet, 0, item->szCode, nLen );


		nLen = (int)sizeof(item->szName);
		TPI_GetFieldValue( hSet, 1, item->szName, nLen );

		m_ZJCls.push_back( item );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}


hfsbool		CKBConn::LoadZJCLSMap(map<hfsstring,hfsstring> &m_ZJCls)
{
	if( !m_ZJCls.empty() )
		return 1;

	int m_ErrCode = IsConnected();

	char szSql[128] = {0};
	sprintf( szSql, "select SYS_FLD_CLASS_CODE,SYS_FLD_CLASS_NAME from  %s where SYS_FLD_CLASS_GRADE=2", "zjcls" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	m_ErrCode  = TPI_GetRecordSetCount( hSet );

	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < m_ErrCode; i ++ )
	{
		ST_ZJCLS *item = new ST_ZJCLS;
		memset( item, 0, sizeof(ST_ZJCLS) );

		nLen = (int)sizeof(item->szCode);
		TPI_GetFieldValue( hSet, 0, item->szCode, nLen );


		nLen = (int)sizeof(item->szName);
		TPI_GetFieldValue( hSet, 1, item->szName, nLen );

		m_ZJCls.insert(make_pair(item->szCode,item->szName));

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsbool		CKBConn::LoadAuthor(vector<ST_AUTHOR*> &m_Author)
{
	//string strSourceTable = "";
	//string strTargetTable = "";
	//strSourceTable = "au_check";
	//strTargetTable = "au_co";
	//CheckData(strSourceTable,strTargetTable);
	//strSourceTable = "au_check2";
	//strTargetTable = "au_co1";
	//CheckData(strSourceTable,strTargetTable);
	//strSourceTable = "au_co1";
	//strTargetTable = "au_co";
	//AppendData(strSourceTable,strTargetTable);
	//strSourceTable = "au_check3";
	//strTargetTable = "au_co2";
	//CheckData(strSourceTable,strTargetTable);
	//strSourceTable = "au_co2";
	//strTargetTable = "au_co";
	//AppendData(strSourceTable,strTargetTable);
	//strSourceTable = "au_check3";
	//strTargetTable = "auth_check";
	//CheckData(strSourceTable,strTargetTable);

	return true;
}

hfsbool		CKBConn::LoadHosp(vector<ST_HOSP*> &m_Hosp)
{
	map<string,string> map;
	std::map<string,string>::iterator it;
	string strLine = "";
	string strNumber = "";
	string strValue = "";
	char line[255] = {0};
	vector<string> items;
    FILE   *fp;  
    char   cChar;  
	bool bInsert = false;
	/*
	string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\yybm2.txt";
	fp=fopen(strFileName.c_str(),"r");  
    int i=0;  
    cChar=fgetc(fp);  
    while(!feof(fp))  
    {  
		if (cChar=='\r')
		{
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			items.clear();
			CUtility::Str()->CStrToStrings(line,items,',');
			int iCount = items.size();
			if (iCount > 1)
			{
				strNumber = items[0];
				strValue = items[1];
				for (int i=2;i< iCount;++i)
				{
					strValue += ",";
					strValue += items[i];
				}
			}
			map.insert(make_pair(strNumber,strValue));
			bInsert = true;
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(fp);  
    }  
	fclose(fp);


	int m_ErrCode = IsConnected();

	char szSql[128] = {0};
	sprintf( szSql, "select 序号 from hosp_datasource");
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	m_ErrCode  = TPI_GetRecordSetCount( hSet );

	long nLen = 20;
	char value[20] = {0};
	TPI_MoveFirst( hSet );
	for( int i = 0; i < m_ErrCode; i ++ )
	{
		memset( value, 0, sizeof(value) );

		nLen = (int)sizeof(value);
		TPI_GetFieldValue( hSet, 0, value, nLen );
		strNumber = value;

		it = map.find(strNumber);
		if (strNumber == "11541")
		{
			int a= 0;
		}
		if (it != map.end())
		{
			strValue = (*it).second;
			UpdateTableHosp(strNumber,strValue,"hosp_datasource");
		}
		TPI_MoveNext( hSet );
	}
	*/


	//string strSourceTable = "";
	//string strTargetTable = "";
	//strSourceTable = "hosp_datasource";
	//strTargetTable = "hosp_check";
	//CheckDataUniv(strSourceTable,strTargetTable);

	//string strSourceTable = "";
	//string strTargetTable = "";
	//strSourceTable = "hosp_check";
	//strTargetTable = "hosp_check1";
	//CheckDataHospUniv(strSourceTable,strTargetTable);
	//strSourceTable = "univ_check";
	//strTargetTable = "univ_check1";
	//CheckDataHospUniv(strSourceTable,strTargetTable);

	return true;
}

hfsbool		CKBConn::LoadUniv(vector<ST_UNIV*> &m_Hosp)
{
	map<string,string> map;
	std::map<string,string>::iterator it;
	string strLine = "";
	string strNumber = "";
	string strValue = "";
	char line[255] = {0};
	vector<string> items;
    FILE   *fp;  
    char   cChar;  
	bool bInsert = false;
	/*
	string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\dxbm2.txt";
	fp=fopen(strFileName.c_str(),"r");  
    int i=0;  
    cChar=fgetc(fp);  
    while(!feof(fp))  
    {  
		if (cChar=='\r')
		{
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			items.clear();
			CUtility::Str()->CStrToStrings(line,items,',');
			int iCount = items.size();
			if (iCount > 1)
			{
				strNumber = items[0];
				strNumber += "?";
				strValue = items[1];
				for (int i=2;i< iCount;++i)
				{
					strValue += ",";
					strValue += items[i];
				}
			}
			map.insert(make_pair(strNumber,strValue));
			bInsert = true;
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(fp);  
    }  
	fclose(fp);


	int m_ErrCode = IsConnected();

	char szSql[128] = {0};
	sprintf( szSql, "select 序号 from univ_datasource");
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	m_ErrCode  = TPI_GetRecordSetCount( hSet );

	long nLen = 20;
	char value[20] = {0};
	TPI_MoveFirst( hSet );
	for( int i = 0; i < m_ErrCode; i ++ )
	{
		memset( value, 0, sizeof(value) );

		nLen = (int)sizeof(value);
		TPI_GetFieldValue( hSet, 0, value, nLen );
		strNumber = value;
		strNumber += "?";
		it = map.find(strNumber);
		//if (strNumber == "11541")
		//{
		//	int a= 0;
		//}
		if (it != map.end())
		{
			strValue = (*it).second;
			UpdateTableHosp(strNumber,strValue,"univ_datasource");
		}
		TPI_MoveNext( hSet );
	}
	*/

	//string strSourceTable = "";
	//string strTargetTable = "";
	//strSourceTable = "univ_datasource";
	//strTargetTable = "univ_check";
	//CheckDataUniv(strSourceTable,strTargetTable);


	return true;
}

hfsbool		CKBConn::CheckData(string strSourceTable,string strTargetTable)
{
	int m_ErrCode = IsConnected();
	int iRecordCount = 0;
	char szSql[1024] = {0};
	sprintf( szSql, "select 作者代码,作者,专题代码,当前单位,机构,是否同一人 from  %s", strSourceTable.c_str() );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	iRecordCount  = TPI_GetRecordSetCount( hSet );
	char zzdm[32] = {0};
	char zz[16] = {0};
	char ztdm[8] = {0};
	char dqdw[255] = {0};
	char jg[255] = {0};
	char yr[8] = {0};
	string strAllAuthorCode = "";
	string strPrevAllAuthorCode = "";
	string strPrevAuthor = "";
	string strCurAuthor = "";
	string strAuthorCode = "";
	string strAuthorName = "";
	string strZtdm = "";
	string strDw = "";
	string strJg = "";
	string strTable = strTargetTable;
	string strPrevZtdm = "";
	string strYr = "";
	long nLen;
	int iCount = 0;
	TPI_MoveFirst( hSet );
	
	for( int i = 0; i < iRecordCount; i ++ )
	{
		memset( zzdm, 0, sizeof(zzdm) );
		memset( zz, 0, sizeof(zz) );
		memset( ztdm, 0, sizeof(ztdm) );
		memset( dqdw, 0, sizeof(dqdw) );
		memset( jg, 0, sizeof(jg) );
		memset( yr, 0, sizeof(yr) );
		TPI_GetFieldValue( hSet, 0, zzdm, 32 );
		strAuthorCode = zzdm;
		TPI_GetFieldValue( hSet, 1, zz, 16 );
		strAuthorName = zz;
		TPI_GetFieldValue( hSet, 2, ztdm, 8 );
		strZtdm = ztdm;
		TPI_GetFieldValue( hSet, 3, dqdw, 255 );
		strDw = dqdw;
		TPI_GetFieldValue( hSet, 4, jg, 255 );
		strJg = jg;
		strCurAuthor = zz;
		TPI_GetFieldValue( hSet, 5, yr, 8 );
		strYr = yr;
		if ("否" == strYr)
		{
			TPI_MoveNext( hSet );
			continue;
		}
		if (strPrevAuthor.compare(strCurAuthor) == 0)
		{
			if (strPrevZtdm.compare(strZtdm) == 0)
			{
				++iCount;
				strAllAuthorCode += zzdm;
				strAllAuthorCode += ";";
				InsertQuery(szSql,strTable,strAuthorCode,strAllAuthorCode,strAuthorName,strJg,strZtdm,strDw,iCount);
				m_ErrCode = IsConnected();
				if(m_ErrCode >= _ERR_SUCCESS){
					m_ErrCode = ExecuteCmd(szSql);
				}
				UpdateQuery(strTable,strAuthorCode,strAllAuthorCode,strZtdm,iCount);
			}
			else
			{
				strPrevZtdm = strZtdm;
				strPrevAuthor = zz;
				iCount = 1;
				InsertQuery(szSql,strTable,strAuthorCode,strAllAuthorCode,strAuthorName,strJg,strZtdm,strDw,1);
				m_ErrCode = IsConnected();
				if(m_ErrCode >= _ERR_SUCCESS){
					m_ErrCode = ExecuteCmd(szSql);
				}
			}
		}
		else
		{
			strPrevZtdm = strZtdm;
			strPrevAuthor = zz;
			iCount = 1;
			strAllAuthorCode = strAuthorCode;
			strAllAuthorCode += ";";
			InsertQuery(szSql,strTable,strAuthorCode,strAllAuthorCode,strAuthorName,strJg,strZtdm,strDw,1);
			m_ErrCode = IsConnected();
			if(m_ErrCode >= _ERR_SUCCESS){
				m_ErrCode = ExecuteCmd(szSql);
			}
		}
		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

void	CKBConn::InsertQuery(phfschar sql,string strTable,string strAuthorCode,string strAllAuthorCode,string strAuthorName,string strJg,string strZtdm,string strDw,int iNum)
{
	char t[10] = {0};
	string strQuery = "INSERT INTO ";
	strQuery += strTable;
	strQuery +="(作者代码,作者所有代码,作者,机构,专题代码,当前单位,作者代码数)";
	strQuery += " VALUES(";
	strQuery += strAuthorCode;
	strQuery += ",";
	strQuery += strAllAuthorCode;
	strQuery += ",";
	strQuery += strAuthorName;
	strQuery += ",";
	if (strJg == "")
	{
		strQuery += "'";
		strQuery += "'";
	}
	else
	{
		strQuery += "'";
		strQuery += strJg;
		strQuery += "'";
	}
	strQuery += ",";
	strQuery += strZtdm;
	strQuery += ",";
	if (strDw == "")
	{
		strQuery += "'";
		strQuery += "'";
	}
	else
	{
		strQuery += "'";
		strQuery += strDw;
		strQuery += "'";
	}
	strQuery += ",";
	memset(t,0,sizeof(t));
	sprintf(t,"%d",iNum);
	strQuery += t;
	strQuery += ")";
	strcpy(sql,strQuery.c_str());
}

hfsint	CKBConn::UpdateQuery(string strTable,string strAuthorCode,string strAllAuthorCode,string strZtdm,int iCount)
{
	string strAuthorCodeAll = "";
	vector<string> authorcode;
	authorcode.clear();
	CUtility::Str()->CStrToStrings(strAllAuthorCode.c_str(),authorcode,';');
	string::size_type   pos(0);     
	vector<string>::iterator it;
	for(it = authorcode.begin(); it != authorcode.end(); ++it){
		strAuthorCodeAll = strAuthorCodeAll + (*it) + "+";
	}
	strAuthorCodeAll = strAuthorCodeAll.substr(0,strAuthorCodeAll.length() - 1);

	m_ErrCode = IsConnected();
	hfschar sql[1024] = {0};
	hfschar cTemp[64]={0};
	hfsstring strSql = "UPDATE ";
	strSql += strTable;
	strSql += " SET 作者所有代码=";
	strSql += strAllAuthorCode;
	strSql += ",作者代码数 =";
	_itoa(iCount,cTemp,10);
	strSql += cTemp;
	strSql += " where 作者代码 = ";
	strSql += strAuthorCodeAll;
	strSql += " and ";
	strSql += " 专题代码 = ";
	strSql += strZtdm;
	strcpy(sql,strSql.c_str());

	hfsint e = ExecuteCmd(sql);

	return e;
}


hfsbool		CKBConn::AppendData(string strSourceTable,string strTargetTable)
{
	int m_ErrCode = IsConnected();
	int iRecordCount = 0;
	char szSql[1024] = {0};
	vector<hfsstring> codeandzt;
	sprintf( szSql, "select 作者代码,作者,专题代码,当前单位,机构 from  %s", strTargetTable.c_str() );

	m_ErrCode = IsConnected();
	if(m_ErrCode >= _ERR_SUCCESS){
		TPI_HRECORDSET hSetThree = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		if(m_ErrCode >= _ERR_SUCCESS && hSetThree){
			hfsint iCountNum = TPI_GetRecordSetCount(hSetThree);
			if  (iCountNum < 0 ?true:false)
			{
				TPI_CloseRecordSet(hSetThree);
			}
			m_ErrCode = TPI_SetRecordCount( hSetThree, 500);
			codeandzt.clear();
			AddVector(hSetThree,codeandzt,iCountNum);
			TPI_CloseRecordSet(hSetThree);
		}
	}

	memset(szSql,0,sizeof(szSql));
	sprintf( szSql, "select 作者代码,作者,专题代码,当前单位,机构,作者所有代码,作者代码数 from  %s", strSourceTable.c_str() );
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	iRecordCount  = TPI_GetRecordSetCount( hSet );
	char zzdm[32] = {0};
	char zz[16] = {0};
	char ztdm[8] = {0};
	char dqdw[255] = {0};
	char jg[255] = {0};
	char zzsydm[255]={0};
	char zzdms[8] = {0};
	string strAllAuthorCode = "";
	string strPrevAllAuthorCode = "";
	string strPrevAuthor = "";
	string strCurAuthor = "";
	string strAuthorCode = "";
	string strAuthorName = "";
	string strZtdm = "";
	string strDw = "";
	string strJg = "";
	string strTable = strTargetTable;
	string strPrevZtdm = "";
	string strCodeAndZt = "";
	long nLen;
	int iCount = 0;
	TPI_MoveFirst( hSet );
	vector<hfsstring>::iterator it;
	for( int i = 0; i < iRecordCount; i ++ )
	{
		memset( zzdm, 0, sizeof(zzdm) );
		memset( zz, 0, sizeof(zz) );
		memset( ztdm, 0, sizeof(ztdm) );
		memset( dqdw, 0, sizeof(dqdw) );
		memset( jg, 0, sizeof(jg) );
		memset( zzsydm, 0, sizeof(zzsydm) );
		memset( zzdms, 0, sizeof(zzdms) );
		TPI_GetFieldValue( hSet, 0, zzdm, 32 );
		strAuthorCode = zzdm;
		TPI_GetFieldValue( hSet, 2, ztdm, 8 );
		strZtdm = ztdm;
		strCodeAndZt = strAuthorCode;
		strCodeAndZt += strZtdm;
		memset(szSql,0,sizeof(szSql));
		sprintf( szSql, "select count(*) from  %s where 作者代码=%s", strSourceTable.c_str(), zzdm);
		m_ErrCode = IsConnected();
		TPI_HRECORDSET hSet2 = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		if( !hSet2 || m_ErrCode < 0 )
		{
			char szMsg[1024];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			if( m_ErrCode >= 0 )
				m_ErrCode = -1;

			return m_ErrCode;
		}

		int iRecordCount1  = TPI_GetRecordSetCount( hSet2 );
		char ccount[4] = {0};
		TPI_GetFieldValue( hSet2, 0, ccount, 32 );
		int iii = (int)_atoi64(ccount);
		if (iii >1)
		{
			int aaa = 0;
		}
		TPI_CloseRecordSet(hSet2);


		it = find(codeandzt.begin(),codeandzt.end(),strCodeAndZt);
		if (it != codeandzt.end())
		{
			TPI_MoveNext(hSet);
			continue;
		}


		TPI_GetFieldValue( hSet, 1, zz, 16 );
		strAuthorName = zz;
		TPI_GetFieldValue( hSet, 3, dqdw, 255 );
		strDw = dqdw;
		TPI_GetFieldValue( hSet, 4, jg, 255 );
		TPI_GetFieldValue( hSet, 5, zzsydm, 255 );
		TPI_GetFieldValue( hSet, 6, zzdms, 8 );
		strJg = jg;
		strCurAuthor = zz;
		strPrevZtdm = strZtdm;
		strPrevAuthor = zz;
		iCount = (int)_atoi64(zzdms);
		strAllAuthorCode = zzsydm;
		InsertQuery(szSql,strTable,strAuthorCode,strAllAuthorCode,strAuthorName,strJg,strZtdm,strDw,iCount);
		m_ErrCode = IsConnected();
		if(m_ErrCode >= _ERR_SUCCESS){
			m_ErrCode = ExecuteCmd(szSql);
		}
		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsint	CKBConn::AddVector(TPI_HRECORDSET &hSet,vector<hfsstring> &vector,hfsint iCountNum)
{
	hfschar t[MAX_QUERY] = {0};
	hfsstring strAuthorCode = "";
	hfsstring strZtCode = "";
	long lLen = 0;
	std::vector<hfsstring>::iterator it;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,MAX_QUERY);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"作者代码",t,MAX_QUERY);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		memset(t,0,MAX_QUERY);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"专题代码",t,MAX_QUERY);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strZtCode = t;
		}
		strAuthorCode += strZtCode;
		it = find(vector.begin(),vector.end(),strAuthorCode);
		if (it == vector.end())
		{
			vector.push_back(strAuthorCode);
		}

		TPI_MoveNext(hSet);
	}
 	return 0;
}

int CKBConn::RunQIKA(DOWN_STATE * taskData)
{   
	int nRet = 0;
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("QIKA_Ref table start time.[%s]",cTime);

	if (taskData->nClearAndInput)
	{
		nRet = StatNew( taskData, "来源代码", "中英文来源", "引文来源代码", szFileName, "来源代码" );
		////nRet = StatNew( taskData, "被引来源代码", "被引中英文来源", "引文来源代码", szFileName, "来源代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\QIKA_REF2014-09-18.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatNewUpdate( taskData, "来源代码", "中英文来源", "引文来源代码", szFileName, "来源代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\ZTCS_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\QIKA_REF2014-09-24.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}


	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("QIKA_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunZTCS(DOWN_STATE * taskData)
{   
	int nRet = 0;
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("ZTCS_Ref table start time.[%s]",cTime);

	if (taskData->nClearAndInput)
	{
		nRet = StatNew( taskData, "专题子栏目代码", "专题代码", "专题子栏目代码", szFileName, "专题子栏目代码" );
		////nRet = StatNew( taskData, "被引专题子栏目代码", "被引专题代码", "被引专题子栏目代码", szFileName, "专题子栏目代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\ZTCS_REF2014-09-22.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\ZTCS_REF2014-06-27.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatNewUpdate( taskData, "专题代码", "专题代码", "专题子栏目代码", szFileName, "专题子栏目代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\ZTCS_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\ZTCS_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}

	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("ZTCS_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunDOCU(DOWN_STATE * taskData)
{   
	int nRet = 0;
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("DOCU_Ref table start time.[%s]",cTime);
	if (taskData->nClearAndInput)
	{
		nRet = StatDoc( taskData,szFileName );

		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\DOCU_REF2014-09-17.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatDocUpdate( taskData,szFileName );

		if( nRet > 0 )
		{
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\DOCU_REF2014-09-23.txt";
			//string strFileName = "z:\\RefServer\\DOCU_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}
	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("DOCU_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunAUTH(DOWN_STATE * taskData)
{
	int nRet = 0;
	AddVectorAuthor();
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Auth_Ref table start time.[%s]",cTime);

	if (taskData->nClearAndInput)
	{
		nRet = StatNew( taskData, "作者代码", "groupcodename(sys_authorcodename)", "minteger", szFileName, "作者代码" );
		////nRet = StatNew( taskData, "被引作者代码", "groupcodename(sys_authorcodename)", "minteger", szFileName, "作者代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\AUTH_REF2014-09-18.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatNewUpdate( taskData, "作者代码", "groupcodename(sys_authorcodename)", "minteger", szFileName, "作者代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\AUTH_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\AUTH_REF2014-09-24.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}

	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Auth_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunHOSP(DOWN_STATE * taskData)
{
	int nRet = 0;
	char szTableName[MAX_PATH] = "hosp_check";
	AddVectorUniv(szTableName);
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("HOSP_Ref table start time.[%s]",cTime);
	char *pWhere = NULL;
	//过滤条件加到从结果里插入map处了
	//if( m_ReUpData == 1 )
	//	pWhere = "单位%医院";

	if (taskData->nClearAndInput)
	{
		nRet = StatNew( taskData, "机构代码", "groupcodename(sys_instcodename)", "minteger", szFileName , "机构代码", pWhere);
		////nRet = StatNew( taskData, "被引机构代码", "groupcodename(sys_instcodename)", "minteger", szFileName , "机构代码", pWhere);
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\HOSP_REF2014-09-17.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatNewUpdate( taskData, "机构代码", "groupcodename(sys_authorcodename)", "minteger", szFileName, "机构代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\HOSP_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\HOSP_REF2014-09-24.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}

	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("HOSP_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunUNIV(DOWN_STATE * taskData)
{
	int nRet = 0;
	char szTableName[MAX_PATH] = "univ_check";
	AddVectorUniv(szTableName);
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Univ_Ref table start time.[%s]",cTime);
	char *pWhere = NULL;
	//过滤条件加到从结果里插入map处了
	//if( m_ReUpData == 1 )
	//	pWhere = "(单位%大学 or 单位%党校 or 单位%学院 or 单位%学校)   not 单位%研究所 not 单位=科学院 not 单位%医院";
	if (taskData->nClearAndInput)
	{
		nRet = StatNew( taskData, "机构代码", "groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
		//目前不用,不要放开////nRet = StatNew( taskData, "被引机构代码", "groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\UNIV_REF2014-09-22.txt";
			//strcpy(szFileName,strFileName.c_str());
			nRet = UpdateTable( taskData->tablename, szFileName );
			nRet = SortTable( taskData->nStatType);
		}
	}
	else
	{
		nRet = StatNewUpdate( taskData, "机构代码", "groupcodename(sys_authorcodename)", "minteger", szFileName, "机构代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\UNIV_REF2014-09-23.txt";
			//strcpy(szFileName,strFileName.c_str());
			//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\UNIV_REF2014-09-24.txt";
			//strcpy(szFileName,strFileName.c_str());
			string strUniqCode = "唯一键值";
			nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
			if( nRet < 0 )
			{
				nRet = -1;
				char szMsg[1024];
				sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
				CUtility::Logger()->d("error info.[%s]",szMsg);
			}
			nRet = SortTable( taskData->nStatType);
		}
	}


	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("UNIV_Ref table end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunSTAT(DOWN_STATE * taskData)
{
	int nRet = 0;
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Stat start time.[%s]",cTime);
	char *pWhere = NULL;
	//nRet = StatZt( taskData, "cjfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//nRet = StatZt( taskData, "cmfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//nRet = StatZt( taskData, "cdfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//nRet = StatZt( taskData, "cpfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//nRet = StatZt( taskData, "ccndtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//nRet = StatZt( taskData, "ipfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
	//InsertLine();
	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Stat end time.[%s]",cTime);

	return nRet;
}

int CKBConn::RunDOWNLOAD(DOWN_STATE * taskData)
{
	CUtility::Logger()->d("RunDOWNLOAD start");
	//ExecTextToKbase(taskData);
	//return ExecFieldUpdate(taskData);
	//return TextToSql();
	//return RunDownLoadTrue(taskData);
	int nRet = 0;
	int nYear,nMonth;
	int nMonthDays;
	nYear = 0;
	nMonth = 0;
	nMonthDays = 0;
	char szYear[5] = {0};
	char szMonth[3] = {0};
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cDay[9] = {0};
	sprintf(cDay,"%04d%02d%02d",systime.wYear,systime.wMonth,systime.wDay);

	if (taskData->nAutoDown == 0)
	{
		memcpy(szYear,taskData->szYearMonth,4);
		memcpy(szMonth,taskData->szYearMonth+4,2);
		nYear = (int)_atoi64(szYear);
		nMonth = (int)_atoi64(szMonth);
	}
	else
	{
		//生成和日期相关的路径，更改taskData->szLocPath
		char cYearMonth[7] = {0};
		if (systime.wMonth == 1)
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear - 1,systime.wMonth - 1);
			nYear = systime.wYear - 1;
			nMonth = systime.wMonth - 1;
		}
		else
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear,systime.wMonth - 1);
			nYear = systime.wYear;
			nMonth = systime.wMonth - 1;
		}

		strcpy(taskData->szLocPath,taskData->szLocDirectory);
		strcat(taskData->szLocPath,"\\");
		strcat(taskData->szLocPath,cYearMonth);
		strcpy(taskData->szYearMonth,cYearMonth);
	}
	nMonthDays = GetMonthDays(nYear, nMonth);
	int nLocFiles = 0;
	while(true)
	{
		GetLocalTime(&systime);
		if (systime.wDay < taskData->nStartDayPerMonth)
		{
			Sleep(5*60*1000);
			continue;
		}
		//判断该月的数据是否都在
		//如果都有，跳出循环
		nLocFiles = GetLocalFiles(taskData);
	
		if (nLocFiles >= nMonthDays)
		{
			int nDownLoadFiles = GetDifferFile(taskData);
			if (nDownLoadFiles > 0)
			{
				for(int i=0;i<nDownLoadFiles;++i)
				{
					Sleep(3*60*1000);//下载一个，3分钟？
				}
			}
			else
			{
				break;
			}
		}
		else
		{
			Sleep(5*60*1000);
		}
	}
	//string szFile = "";
	//szFile = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\2014-08-19-201312.txt";
	//nRet = AppendTable( taskData->tablename, szFile.c_str() );
	CUtility::Logger()->d("RunDOWNLOAD end");
	CUtility::Logger()->d("RunDownLoadTrue start");

	return RunDownLoadTrue(taskData);
}

int CKBConn::RunTextGet(DOWN_STATE * taskData)
{
	CUtility::Logger()->d("RunDOWNLOAD start");
	//ExecTextToKbase(taskData);
	//return ExecFieldUpdate(taskData);
	//return TextToSql();
	if (taskData->nDowithErrorZip)
	{
		if (taskData->nDowithErrorZip == 2)
		{
			return RunTextGetTrue2(taskData);
		}
		if (taskData->nDowithErrorZip == 5)
		{
			return RunTextGetTrueTm(taskData);
		}
		if (taskData->nDowithErrorZip == 1)
		{
			return RunTextGetTrue1(taskData);
		}
		if (taskData->nDowithErrorZip == 3)
		{
			return RunTextGetTrue3(taskData);
		}
		if (taskData->nDowithErrorZip == 4)
		{
			return RunTextGetTrue4(taskData);
		}
		if (taskData->nDowithErrorZip == 6)
		{
			return RunTextGetTrueTm5(taskData);
		}
		if (taskData->nDowithErrorZip == 7)
		{
			return RunTextGetTrueTm7(taskData);
		}
		if (taskData->nDowithErrorZip == 8)
		{
			return RunTextGetTrueTm8(taskData);
		}
		if (taskData->nDowithErrorZip == 9)
		{
			return RunTextGetTrueTm9(taskData);
		}
		if (taskData->nDowithErrorZip == 10)
		{
			return RunTextGetTrueTm10(taskData);
		}
		if (taskData->nDowithErrorZip == 11)
		{
			return RunTextGetTrueTm11(taskData);
		}
		if (taskData->nDowithErrorZip == 12)
		{
			return RunTextGetTrueTm11(taskData);
		}
		if (taskData->nDowithErrorZip == 13)
		{
			return GetTwoTxtComm(taskData);
		}
		if (taskData->nDowithErrorZip == 14)
		{
			return WriteXmlToKbase(taskData);
		}
		if (taskData->nDowithErrorZip == 15)
		{
			return RenameFileName(taskData);
		}
	}
	else
	{
		return RunTextGetTrue(taskData);
	}
	int nRet = 0;
	int nYear,nMonth;
	int nMonthDays;
	nYear = 0;
	nMonth = 0;
	nMonthDays = 0;
	char szYear[5] = {0};
	char szMonth[3] = {0};
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cDay[9] = {0};
	sprintf(cDay,"%04d%02d%02d",systime.wYear,systime.wMonth,systime.wDay);

	if (taskData->nAutoDown == 0)
	{
		memcpy(szYear,taskData->szYearMonth,4);
		memcpy(szMonth,taskData->szYearMonth+4,2);
		nYear = (int)_atoi64(szYear);
		nMonth = (int)_atoi64(szMonth);
	}
	else
	{
		//生成和日期相关的路径，更改taskData->szLocPath
		char cYearMonth[7] = {0};
		if (systime.wMonth == 1)
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear - 1,systime.wMonth - 1);
			nYear = systime.wYear - 1;
			nMonth = systime.wMonth - 1;
		}
		else
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear,systime.wMonth - 1);
			nYear = systime.wYear;
			nMonth = systime.wMonth - 1;
		}

		strcpy(taskData->szLocPath,taskData->szLocDirectory);
		strcat(taskData->szLocPath,"\\");
		strcat(taskData->szLocPath,cYearMonth);
		strcpy(taskData->szYearMonth,cYearMonth);
	}
	nMonthDays = GetMonthDays(nYear, nMonth);
	int nLocFiles = 0;
	while(true)
	{
		GetLocalTime(&systime);
		if (systime.wDay < taskData->nStartDayPerMonth)
		{
			Sleep(5*60*1000);
			continue;
		}
		//判断该月的数据是否都在
		//如果都有，跳出循环
		nLocFiles = GetLocalFiles(taskData);
	
		if (nLocFiles >= nMonthDays)
		{
			int nDownLoadFiles = GetDifferFile(taskData);
			if (nDownLoadFiles > 0)
			{
				for(int i=0;i<nDownLoadFiles;++i)
				{
					Sleep(3*60*1000);//下载一个，3分钟？
				}
			}
			else
			{
				break;
			}
		}
		else
		{
			Sleep(5*60*1000);
		}
	}
	//string szFile = "";
	//szFile = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\2014-08-19-201312.txt";
	//nRet = AppendTable( taskData->tablename, szFile.c_str() );
	CUtility::Logger()->d("RunDOWNLOAD end");
	CUtility::Logger()->d("RunDownLoadTrue start");

	return RunDownLoadTrue(taskData);
}

int CKBConn::RunDICT(DOWN_STATE * taskData)
{
	int nRet = 0;
	int nYear,nMonth;
	int nMonthDays;
	nYear = 0;
	nMonth = 0;
	nMonthDays = 0;
	char szYear[5] = {0};
	char szMonth[3] = {0};
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cDay[9] = {0};
	sprintf(cDay,"%04d%02d%02d",systime.wYear,systime.wMonth,systime.wDay);

	if (taskData->nAutoDown == 0)
	{
		memcpy(szYear,taskData->szYearMonth,4);
		memcpy(szMonth,taskData->szYearMonth+4,2);
		nYear = (int)_atoi64(szYear);
		nMonth = (int)_atoi64(szMonth);
	}
	else
	{
		//生成和日期相关的路径，更改taskData->szLocPath
		char cYearMonth[7] = {0};
		if (systime.wMonth == 1)
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear - 1,systime.wMonth - 1);
			nYear = systime.wYear - 1;
			nMonth = systime.wMonth - 1;
		}
		else
		{
			sprintf(cYearMonth,"%04d%02d%",systime.wYear,systime.wMonth - 1);
			nYear = systime.wYear;
			nMonth = systime.wMonth - 1;
		}

		strcpy(taskData->szLocPath,taskData->szLocDirectory);
		strcat(taskData->szLocPath,"\\");
		strcat(taskData->szLocPath,cYearMonth);
		strcpy(taskData->szYearMonth,cYearMonth);
	}
	nMonthDays = GetMonthDays(nYear, nMonth);
	int nLocFiles = 0;
	while(true)
	{
		GetLocalTime(&systime);
		if (systime.wDay < taskData->nStartDayPerMonth)
		{
			Sleep(5*60*1000);
			continue;
		}
		//判断该月的数据是否都在
		//如果都有，跳出循环
		nLocFiles = GetLocalFiles(taskData);
	
		if (nLocFiles >= nMonthDays)
		{
			int nDownLoadFiles = GetDifferFile(taskData);
			if (nDownLoadFiles > 0)
			{
				for(int i=0;i<nDownLoadFiles;++i)
				{
					Sleep(3*60*1000);//下载一个，3分钟？
				}
			}
			else
			{
				break;
			}
		}
		else
		{
			Sleep(5*60*1000);
		}
	}
	//string szFile = "";
	//szFile = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\2014-08-19-201312.txt";
	//nRet = AppendTable( taskData->tablename, szFile.c_str() );

	return RunDownLoad(taskData);
}

int CKBConn::RunCOLUHIER(DOWN_STATE * taskData)
{   
	int nRet = 0;
	char szFileName[MAX_PATH]={0};
	m_ReUpData = 1;
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Colu_hier start time.[%s]",cTime);

	//if (taskData->nClearAndInput)
	{
		//nRet = StatZt( taskData, "cjfdtotal", "","groupcodename(sys_instcodename)", "minteger", szFileName, "机构代码" ,pWhere);
		//int CKBConn::StatColumn(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)

		//nRet = StatColumn( taskData, "", "期刊栏目层次", "期刊栏目层次", "", szFileName, "" , "");
		////nRet = StatNew( taskData, "被引来源代码", "被引中英文来源", "引文来源代码", szFileName, "来源代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\QIKA_REF2014-09-18.txt";
			//strcpy(szFileName,strFileName.c_str());
			//nRet = UpdateTable( taskData->tablename, szFileName );
			//nRet = SortTable( taskData->nStatType);
		}
		//nRet = StatColumn_NQ( taskData, "KMCOLUHIER_NQ", "期刊栏目层次", "THNAME", "", szFileName, "" , "");
		////nRet = StatNew( taskData, "被引来源代码", "被引中英文来源", "引文来源代码", szFileName, "来源代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\QIKA_REF2014-09-18.txt";
			//strcpy(szFileName,strFileName.c_str());
			//nRet = UpdateTable( taskData->tablename, szFileName );
			//nRet = SortTable( taskData->nStatType);
		}
		nRet = StatColumn_168( taskData, "KMCOLUHIER_168", "期刊栏目层次", "期刊栏目层次", "", szFileName, "" , "");
		////nRet = StatNew( taskData, "被引来源代码", "被引中英文来源", "引文来源代码", szFileName, "来源代码" );
		if( nRet > 0 )
		{
			//string strFileName = "d:\\RefServer\\QIKA_REF2014-09-18.txt";
			//strcpy(szFileName,strFileName.c_str());
			//nRet = UpdateTable( taskData->tablename, szFileName );
			//nRet = SortTable( taskData->nStatType);
		}
	}
	//else
	//{
	//	nRet = StatNewUpdate( taskData, "来源代码", "中英文来源", "引文来源代码", szFileName, "来源代码" );
	//	if( nRet > 0 )
	//	{
	//		//string strFileName = "d:\\RefServer\\ZTCS_REF2014-09-23.txt";
	//		//strcpy(szFileName,strFileName.c_str());
	//		//string strFileName = "E:\\work\\RefServer\\src\\RefServer\\DownLoadCounter\\Debug\\QIKA_REF2014-09-24.txt";
	//		//strcpy(szFileName,strFileName.c_str());
	//		string strUniqCode = "唯一键值";
	//		nRet = UpdateTableTrue( taskData->tablename, szFileName, strUniqCode.c_str() );
	//		if( nRet < 0 )
	//		{
	//			nRet = -1;
	//			char szMsg[1024];
	//			sprintf( szMsg, "UPDATE %s to %s failed!", szFileName, m_OutFileName.szFN_DOCU );
	//			CUtility::Logger()->d("error info.[%s]",szMsg);
	//		}
	//		nRet = SortTable( taskData->nStatType);
	//	}
	//}


	GetLocalTime(&systime);
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
	CUtility::Logger()->d("Colu_hier end time.[%s]",cTime);

	return nRet;
}

bool CKBConn::IsValidata(char *pName, char *pTable)
{
	bool bRet = true;
	
	if( !stricmp( pTable, m_OutFileName.szFN_UNIV ) )//(单位%大学 or 单位%党校 or 单位%学院 or 单位%学校)   not 单位%研究所 not 单位=科学院 not 单位%医院 
	{
		if( strstr( pName, "研究所") || strstr( pName, "科学院") || strstr( pName, "医院" ) )
			bRet = false;
		else if( !strstr( pName, "大学") && !strstr( pName, "学院") && !strstr( pName, "党校") && !strstr( pName, "学校") )
			bRet = false;
	}
	else if( !stricmp( pTable, m_OutFileName.szFN_HOSP ) )
	{
		 if( !strstr( pName, "医院") )
			 bRet = false;
	}

	return bRet;
}

//H指数
int CKBConn::GetHIndex(const char *pSql)
{
	int nRet = 0;

	int nErrCode = 0;
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", pSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

		return m_ErrCode;
	}
	int nCount = TPI_GetRecordSetCount( hSet );
	if( nCount > 0 )
	{
		TPI_MoveFirst( hSet );

		long nLen=64;
		char szBuff[64];
		int nRefTimes;
		
		int nNo;
		for( int i = 0; i < nCount; i ++ )
	    {
            nLen = 64;
			memset( szBuff, 0, nLen );
			TPI_GetFieldValueByName( hSet, "被引频次", szBuff, nLen );  
			if( szBuff[0] == '\0' )
			{
				TPI_MoveNext( hSet );
				continue;
			}

			nNo = i + 1;
            nRefTimes  = (int)_atoi64( szBuff );

            if( nNo > nRefTimes )
			{
				nRet = nNo - 1;
				break;
			}

			TPI_MoveNext( hSet );
		} 
	}
	TPI_CloseRecordSet( hSet );

	return nRet;
}

int CKBConn::GetTimes(const char *pSql)
{
	int nRet = 0;

	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[ sql %s]",pSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", pSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	if( TPI_GetRecordSetCount( hSet ) > 0 )
	{
		TPI_MoveFirst( hSet );

		long nLen = 64;
		char szBuff[64];
		memset( szBuff, 0, nLen );
		TPI_GetFieldValue( hSet, 0, szBuff, nLen );
		if( szBuff[0] != '\0' )
			nRet = _atoi64( szBuff );
	}
	TPI_CloseRecordSet( hSet );

	return nRet;
}

int CKBConn::GetTimes2(const char *pSql,long long &item0,long long &item1)
{
	int nRet = 0;

	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[ sql %s]",pSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", pSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);

		return m_ErrCode;
	}
	if( TPI_GetRecordSetCount( hSet ) > 0 )
	{
		TPI_MoveFirst( hSet );

		long nLen = 64;
		char szBuff[64];
		memset( szBuff, 0, nLen );
		TPI_GetFieldValue( hSet, 0, szBuff, nLen );
		if( szBuff[0] != '\0' )
			item0 = _atoi64( szBuff );
		memset( szBuff, 0, nLen );
		TPI_GetFieldValue( hSet, 1, szBuff, nLen );
		if( szBuff[1] != '\0' )
			item1 = _atoi64( szBuff );
	}
	TPI_CloseRecordSet( hSet );

	return m_ErrCode;
}

int CKBConn::GetResult(const char *pSql,string &strResult)
{
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", pSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);

		return m_ErrCode;
	}
	if( TPI_GetRecordSetCount( hSet ) > 0 )
	{
		TPI_MoveFirst( hSet );

		long nLen = 128;
		char szBuff[128];
		memset( szBuff, 0, nLen );
		TPI_GetFieldValue( hSet, 0, szBuff, nLen );
		strResult = szBuff;
	}
	TPI_CloseRecordSet( hSet );

	return m_ErrCode;
}

int CKBConn::WriteRecFile(FILE *pFile, char *pFieldName, const char *pData, bool bSameRow)
{
	if( !pFile || !pFieldName || !pData )
		return -1;
	try
	{
		if( !bSameRow )
			fwrite( "<REC>\n", 1, 6, pFile );
		fwrite( "<", 1, 1, pFile );
		fwrite( pFieldName, 1, (int)strlen(pFieldName), pFile );
		fwrite( ">=", 1, 2, pFile );
		fwrite( pData, 1, (int)strlen(pData), pFile );
		fwrite( "\n", 1, 1, pFile );
	}
	catch(...)
	{
		return -1;
	}
	
	return 0;
}

int CKBConn::WriteRecFile(FILE *pFile, char *pFieldName, int nNum, bool bSameRow)
{
	if( !pFile || !pFieldName )
		return -1;

	if( nNum <= 0 )
		return 0;

	try
	{
		if( !bSameRow )
			fwrite( "<REC>\n", 1, 6, pFile );
		fwrite( "<", 1, 1, pFile );
		fwrite( pFieldName, 1, (int)strlen(pFieldName), pFile );
		fwrite( ">=", 1, 2, pFile );

		char szBuff[64];
		memset( szBuff, 0, sizeof(szBuff) );
		itoa( nNum, szBuff, 10 );
		fwrite( szBuff, 1, (int)strlen(szBuff), pFile );
		fwrite( "\n", 1, 1, pFile );
	}
	catch(...)
	{
		return -1;
	}
	
	return 0;
}

int CKBConn::WriteRecFileFloat(FILE *pFile, char *pFieldName, float nNum, bool bSameRow)
{
	if( !pFile || !pFieldName )
		return -1;

	if( nNum <= 0 )
		return 0;

	try
	{
		if( !bSameRow )
			fwrite( "<REC>\n", 1, 6, pFile );
		fwrite( "<", 1, 1, pFile );
		fwrite( pFieldName, 1, (int)strlen(pFieldName), pFile );
		fwrite( ">=", 1, 2, pFile );

		char szBuff[64];
		memset( szBuff, 0, sizeof(szBuff) );
		sprintf(szBuff,"%.2f",nNum);
		fwrite( szBuff, 1, (int)strlen(szBuff), pFile );
		fwrite( "\n", 1, 1, pFile );
	}
	catch(...)
	{
		return -1;
	}
	
	return 0;
}

FILE *CKBConn::OpenRecFile(const char *pFileName)
{
	FILE *pFile = NULL;
	try
	{
		::DeleteFile( pFileName );
		pFile = fopen( pFileName, "w" );
	}
	catch(...)
	{
		pFile = NULL;
	}

	return pFile;
}

int CKBConn::CloseRecFile(FILE *pFile)
{
	try
	{
		fclose( pFile );
	}
	catch(...)
	{
		return -1;
	}

	return 0;
}

int CKBConn::AddVectorAuthor()
{
	TPI_HRECORDSET hSet;
	char szSql[MAX_SQL]= {0};
	sprintf( szSql, "select 作者代码 as Code,专题代码 as ZtCode from auth_check order by 专题代码,作者代码" );

	m_ErrCode = IsConnected();
	if(m_ErrCode >= _ERR_SUCCESS){
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		if(m_ErrCode >= _ERR_SUCCESS && hSet){
			hfsint iCountNum = TPI_GetRecordSetCount(hSet);
			if  (iCountNum <= 0 ?true:false)
			{
				TPI_CloseRecordSet(hSet);
			}
			m_ErrCode = TPI_SetRecordCount( hSet, 500);
			m_ZtCodeAuthorCode.clear();
			AddMapAuthor(hSet,m_ZtCodeAuthorCode,iCountNum);
			TPI_CloseRecordSet(hSet);
		}
	}
	return 0;
}

hfsint	CKBConn::AddMapAuthor(TPI_HRECORDSET &hSet,map<hfsstring,hfsstring> &map,hfsint iCountNum)
{
	hfschar t[64] = {0};
	hfsstring strZtCode = "";
	hfsstring strCurrentZtCode = "";
	hfsstring strPrevZtCode = "";
	hfsstring strAuthorCode = "";
	hfsstring strAllAuthorCode = "";
	long lLen = 0;
	std::map<hfsstring,hfsstring>::iterator it;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"ZtCode",t,64);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strZtCode = t;
		}
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Code",t,64);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		if ((strPrevZtCode != strZtCode)&&(strPrevZtCode != ""))
		{
			it = map.find(strPrevZtCode);
			if(it == map.end()){
				if (strAllAuthorCode.length() > 0)
				{
					strAllAuthorCode = strAllAuthorCode.substr(0,strAllAuthorCode.length()-1);
				}
				map.insert(make_pair(strPrevZtCode,strAllAuthorCode));
			}
			strPrevZtCode = strZtCode;
			strAllAuthorCode = strAuthorCode;
			strAllAuthorCode += "+";
		}
		else
		{

			strPrevZtCode = strZtCode;
			strAllAuthorCode += strAuthorCode;
			strAllAuthorCode += "+";
		}
		TPI_MoveNext(hSet);
	}
	it = map.find(strPrevZtCode);
	if(it == map.end()){
		if (strAllAuthorCode.length() > 0)
		{
			strAllAuthorCode = strAllAuthorCode.substr(0,strAllAuthorCode.length()-1);
		}
		map.insert(make_pair(strZtCode,strAllAuthorCode));
	}
	return 1;		
}

int CKBConn::AddVectorUniv(char * szTable)
{
	TPI_HRECORDSET hSet;
	char szSql[MAX_SQL]= {0};
	sprintf( szSql, "select 机构代码 as Code,机构所有代码 as AllCode,机构代码数 as CodeNums from %s order by 机构代码数 desc" ,szTable);

	m_ErrCode = IsConnected();
	if(m_ErrCode >= _ERR_SUCCESS){
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		if(m_ErrCode >= _ERR_SUCCESS && hSet){
			hfsint iCountNum = TPI_GetRecordSetCount(hSet);
			if  (iCountNum <= 0 ?true:false)
			{
				TPI_CloseRecordSet(hSet);
			}
			m_ErrCode = TPI_SetRecordCount( hSet, 500);
			m_UnivCodeMulti.clear();
			AddMapUniv(hSet,m_UnivCodeMulti,iCountNum);
			TPI_CloseRecordSet(hSet);
		}
	}
	return 0;
}

hfsint	CKBConn::AddMapUniv(TPI_HRECORDSET &hSet,map<int,hfsstring> &map,hfsint iCountNum)
{
	hfschar t[512] = {0};
	hfsstring strCode = "";
	hfsstring strCurrAllCode = "";
	hfsstring strAllCode = "";
	int nCodeNums = 0;
	int nCountCode = 0;
	int nMaps = 0;
	int lLen = 0;
	std::map<hfsstring,hfsstring>::iterator it;
	for (int i = 1;i <= iCountNum;++i){
		lLen = (int)(sizeof(t));
		memset(t,0,lLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Code",t,lLen);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCode = t;
		}
		memset(t,0,lLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"AllCode",t,lLen);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCurrAllCode = t;
			strAllCode += strCurrAllCode;
		}
		memset(t,0,lLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"CodeNums",t,lLen);	
		if(m_ErrCode < 0 ){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			nCodeNums = _atoi64(t);
			nCountCode += nCodeNums;
		}
		if (nCountCode >= 200)
		{
			++nMaps;
			map.insert(make_pair(nMaps,strAllCode));
			nCountCode = 0;
			strAllCode = "";
		}

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int CKBConn::StatNew(DOWN_STATE * taskData, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	char pTable[255] = {0};
	strcpy(pTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, pTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	ST_ITEM item;
	char szSql[MAX_SQL], szBuff[64];
	vector<ST_ZJCLS *>::iterator it;
	std::map<hfsstring,string>::iterator itt;
	std::map<hfsstring,int>::iterator iti;
	string strZtCode = "";
	string strAuthorCode = "";
	vector<pair<string,int>> tmpVector;  
	vector<pair<string,int>> tVector;  
	vector<string> items;
	string::size_type   pos(0);     
	string strCode = "";
	string strPykm = "";
	string strResult = "";
	string strUniqField = "";
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	char cTime[64] = {0};
	nType = taskData->nStatType;
	for( it =  m_ZJCls.begin(); it !=  m_ZJCls.end(); ++ it )
	{
		//string strTemp1 = (*it)->szCode;
		//if (strTemp1 != "A011")
		//{
		//	continue;
		//}
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("loop start time.[%s]",cTime);
		}
		CUtility::Logger()->d("doing.[%s]",(*it)->szCode);
		//string strTemp = (*it)->szCode;
		//if (strTemp == "H132")
		//{
		//	int a = 0;
		//	int b = 0;
		//}
		//else
		//{
		//	continue;
		//}
		nRet = 0;
		switch(nType)    
		{
			case REF_RUNQIKAONLY:	//高被引期刊
			case REF_RUNAUTHONLY:	//高被引作者
			case REF_RUNUNIVONLY:	//高被引院校
				if( m_ReUpData == 0 )
					sprintf( szSql, "select Code, Name from %s where  专题代码=%s", pTable, (*it)->szCode );
				else
				{
					if( pWhere == NULL )
					{
						sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode, pField, pDict );
						//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", (*it)->szCode, pField, pDict );
					}
					else
					{
						if( !stricmp( pTable, m_OutFileName.szFN_HOSP ) )
						{
							 if( strchr( (*it)->szCode, 'E' ) )
								sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode, pWhere,  pField, pDict );
								//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", (*it)->szCode, pWhere,  pField, pDict );
							 else
								 continue;
						}
						else
							sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode, pWhere,  pField, pDict );
							//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", (*it)->szCode, pWhere,  pField, pDict );
					}
				}
				break;

			case REF_RUNHOSPONLY:	//高被引医院
				if( m_ReUpData == 0 )
					sprintf( szSql, "select Code, Name from %s where  专题代码=%s", pTable, (*it)->szCode );
				else
				{
					if( strchr( (*it)->szCode, 'E' ) )
					sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode, pField, pDict );
					//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", (*it)->szCode, pField, pDict );
					else
						continue;
				}
				break;

			case REF_RUNZTCSONLY:	//高被引专题
				if( m_ReUpData == 0 )
					sprintf( szSql, "select Code, Name from %s where  专题代码=%s", pTable, (*it)->szCode );
				else
				{
					if( pWhere == NULL )
					{
						sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode );
						//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s?", pField, pCodeName, "cojdref", (*it)->szCode );
					}
					else
					{
						sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s order by sum(被引频次) desc", pField, pCodeName, "cjfdref", (*it)->szCode, pWhere );
						//sprintf( szSql, "select %s as Code,groupcodename(%s) as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s", pField, pCodeName, "cojdref", (*it)->szCode, pWhere );
					}
				}
				break;
            
			case REF_RUNDOCUONLY:	//高被引文献
				break;

			default:
				break;
		}


		m_ErrCode = IsConnected();
		TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);

			CloseRecFile( hFile );
			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("TYPE[%d]执行[%s]，结果集为0",nType,szSql);
		}
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		TPI_MoveFirst( hSet );
		m_AuthorCodeCitedNum.clear();
		m_AuthorCodeCitedNumMiddle.clear();
		AddMapAuthorCitedNum(hSet,m_AuthorCodeCitedNum,nRecordCount,pTable);
		TPI_CloseRecordSet( hSet );
		switch(nType)    
		{
			case REF_RUNQIKAONLY:	//高被引期刊
				break;

			case REF_RUNAUTHONLY:	//高被引作者
				m_ErrCode = AddAuthorMulti((*it)->szCode,pTable,pWhere,pField,pCodeName,pDict);
				break;

			case REF_RUNHOSPONLY:	//高被引医院
				{
					char pSourceTable[MAX_PATH] = "hosp_check";
					m_ErrCode = AddUnivMulti((*it)->szCode,pTable,pWhere,pField,pCodeName,pDict,pSourceTable);
				}
				break;
            
			case REF_RUNUNIVONLY:	//高被引院校
				{
					char pSourceTable[MAX_PATH] = "univ_check";
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddUnivMulti start time.[%s]",cTime);
					}
					m_ErrCode = AddUnivMulti((*it)->szCode,pTable,pWhere,pField,pCodeName,pDict,pSourceTable);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddUnivMulti end time.[%s]",cTime);
					}
				}
				break;

			case REF_RUNZTCSONLY:	//高被引专题
				break;
            
			case REF_RUNDOCUONLY:	//高被引文献
				break;

			default:
				break;
		}
		if( m_ErrCode < 0 )
		{
			CloseRecFile( hFile );
			return m_ErrCode;
		}

		tVector.clear();
		switch(nType)    
		{
			case REF_RUNHOSPONLY:	//高被引医院
			case REF_RUNAUTHONLY:	//高被引作者
			case REF_RUNUNIVONLY:	//高被引院校
				{
					tmpVector.clear();
					sortMapByValue(m_AuthorCodeCitedNum,tmpVector); 
					//从tVector中取前150个去重记录
					UniqVectorItem(tmpVector,tVector); 
				}
				break;
			case REF_RUNZTCSONLY:	//高被引专题
			case REF_RUNQIKAONLY:	//高被引期刊
			case REF_RUNDOCUONLY:	//高被引文献
				{
					tVector.clear();
					sortMapByValue(m_AuthorCodeCitedNum,tVector); 
				}
				break;

			default:
				break;
		}
		nRecordCount = tVector.size();
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("tVector的大小为0");
		}
		if (nRecordCount > GETRECORDNUM*1.5)
		{
			nRecordCount = GETRECORDNUM*1.5;
		}

		switch(nType)    
		{
			case REF_RUNQIKAONLY:	//高被引期刊
				break;

			case REF_RUNAUTHONLY:	//高被引作者
			case REF_RUNHOSPONLY:	//高被引医院
				{
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddAuthItem start time.[%s]",cTime);
					}
					string strCodeAll = "";
					int nItems = 0;
					for( int i = 0; i < nRecordCount; ++i )
					{
						items.clear();
						CUtility::Str()->CStrToStrings(tVector[i].first.c_str(),items,';');
						nItems = items.size();
						if (nItems > 0)
						{
							strCodeAll += items[0];
							strCodeAll += "+";
						}
					
					}
					nItems = strCodeAll.length();
					if (nItems >= 1)
					{
						strCodeAll = strCodeAll.substr(0,nItems - 1);
					}
					//m_ErrCode = AddAuthItem((*it)->szCode,pTable,strCodeAll,pField2,pCodeName,pDict);
					m_ErrCode = AddUnivItem((*it)->szCode,pTable,strCodeAll,pField2,pCodeName,pDict);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddAuthItem end time.[%s]",cTime);
					}
				}
				break;
			case REF_RUNUNIVONLY:	//高被引院校
				{
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddUnivItem start time.[%s]",cTime);
					}
					string strCodeAll = "";
					int nItems = 0;
					for( int i = 0; i < nRecordCount; ++i )
					{
						items.clear();
						CUtility::Str()->CStrToStrings(tVector[i].first.c_str(),items,';');
						nItems = items.size();
						if (nItems > 0)
						{
							strCodeAll += items[0];
							strCodeAll += "+";
						}
					
					}
					nItems = strCodeAll.length();
					if (nItems >= 1)
					{
						strCodeAll = strCodeAll.substr(0,nItems - 1);
					}
					m_ErrCode = AddUnivItem((*it)->szCode,pTable,strCodeAll,pField2,pCodeName,pDict);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("AddUnivItem end time.[%s]",cTime);
					}
				}
				break;

			case REF_RUNZTCSONLY:	//高被引专题
				break;
            
			case REF_RUNDOCUONLY:	//高被引文献
				break;

			default:
				break;
		}

		if( m_ErrCode < 0 )
		{
			CloseRecFile( hFile );
			return m_ErrCode;
		}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		for( int i = 0; i < nRecordCount; i ++ )
		{
			
			memset( &item, 0, sizeof(ST_ITEM) );
			
			items.clear();
			CUtility::Str()->CStrToStrings(tVector[i].first.c_str(),items,';');
			if (items.size() < 2)
			{
				continue;
			}
			//Code
			strcpy(item.szCode,items[0].c_str());
			//Name
			strcpy(item.szName,items[1].c_str());

			switch(nType)    
			{
				case REF_RUNQIKAONLY:	//高被引期刊
					{
						//sprintf( szSql, "select sum(被引频次) from %s where %s='%s' and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
						//item.nRefTimes = GetTimes( szSql );

						sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
						GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

						if( item.nRefTimes <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
							continue;
						}
						long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
						item.nTRefRate = ((float)(nFm+5))/1000;
						int nRate = item.nTRefRate*100;
						item.nTRefRate = (float)nRate/100;

						strCode = item.szCode;
						if( (pos=strCode.find("_"))!=string::npos )
						{
							strPykm = strCode.substr(pos+1,strCode.length());
						}
						//载文量
						sprintf( szSql, "select count(*) from %s where %s='%s' and 专题子栏目代码=%s?", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						item.nTitles = GetTimes( szSql );
						if( item.nTitles <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
							continue;
						}

						//下载量
						//sprintf( szSql, "select sum(下载频次) from %s where %s='%s' and 专题子栏目代码=%s?", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						//item.nDownTimes = GetTimes( szSql );

						//H指数
						//sprintf( szSql, "select 被引频次 from %s where %s='%s' and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						sprintf( szSql, "select 被引频次 from %s where %s='%s' and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
						//自社科
						sprintf( szSql, "select top 1 自社科 as result from %s where %s='%s'", "cjfdref", pField2, item.szCode );
						GetResult( szSql,strResult );
						if( m_ReUpData == 0 )
						{
							//nLen = (int)sizeof(item.szID);
							//nLen = TPI_GetFieldValueByName( hSet, "Code", item.szID, nLen );
							WriteRecFile( hFile, "Name", item.szName, false );
							WriteRecFile( hFile, "Code", item.szCode, true );
						}
						else
						{
							WriteRecFile( hFile, "Name", item.szName, false );
							WriteRecFile( hFile, "PYKM", strPykm.c_str(), true );
							WriteRecFile( hFile, "自社科", strResult.c_str(), true );
						}
						WriteRecFile( hFile, "发文量", item.nTitles, true );
					}
					break;

				case REF_RUNAUTHONLY:	//高被引作者
					{
					//被引频次
					//sprintf( szSql, "select sum(被引频次) from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nRefTimes = GetTimes( szSql );

					//他引频次
					//sprintf( szSql, "select sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nTRefTimes = GetTimes( szSql );
					//被引频次、他引频次
					sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

					if( item.nRefTimes <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
						continue;
					}
					long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
					item.nTRefRate = ((float)(nFm+5))/1000;
					int nRate = item.nTRefRate*100;
					item.nTRefRate = (float)nRate/100;

					//载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivZwl,item.nTitles);
					if( item.nTitles <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
						continue;
					}
                    
					//下载量
					//sprintf( szSql, "select sum(下载频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					//item.nDownTimes = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivXzl,item.nDownTimes);

					//H指数
					//sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHIndex = GetHIndex( szSql );
					//核心期刊载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nHXTitles = GetTimes( szSql );
					GetItemsNumber(item.szCode,m_UnivHx,item.nHXTitles);
					//SCI载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nSCITitles = GetTimes( szSql );
					GetItemsNumber(item.szCode,m_UnivSci,item.nSCITitles);
					//EI载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and EI收录刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nEITitles = GetTimes( szSql );
					GetItemsNumber(item.szCode,m_UnivEi,item.nEITitles);
					//基金论文数
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nFundTitles = GetTimes( szSql );
					GetItemsNumber(item.szCode,m_UnivFun,item.nFundTitles);

					if( m_ReUpData == 0 )
					{
						nLen = (int)sizeof(item.szID);
						nLen = TPI_GetFieldValueByName( hSet, "Code", item.szID, nLen );
						WriteRecFile( hFile, "ID", item.szID, false );

						WriteRecFile( hFile, "Code", item.szCode, true );
					}
					else
					{
						WriteRecFile( hFile, "Code", item.szCode, false );
					}
					WriteRecFile( hFile, "Name", item.szName, true );
					WriteRecFile( hFile, "发文量", item.nTitles, true );
					}
					break;

				case REF_RUNHOSPONLY:	//高被引医院
				case REF_RUNUNIVONLY:	//高被引院校
					{
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("被引频次 start time.[%s]",cTime);
					}
					//被引频次
					//sprintf( szSql, "select sum(被引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nRefTimes = GetTimes( szSql );

					//被引频次、他引频次
					sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );
					if (item.nRefTimes == 0)
					{
						CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
						continue;
					}
					long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
					item.nTRefRate = ((float)(nFm+5))/1000;
					int nRate = item.nTRefRate*100;
					item.nTRefRate = (float)nRate/100;

					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("被引频次 end time.[%s]",cTime);
					}
					//载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nTitles = GetTimes( szSql );
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("载文量 start time.[%s]",cTime);
					}
					GetItemsNumber(item.szCode,m_UnivZwl,item.nTitles);
					if( item.nTitles <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
						continue;
					}
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("载文量 end time.[%s]",cTime);
					}

					//下载量  因为超时的原因，改成先计算而时间增加
					//if (taskData->nLog)
					//{
					//	GetLocalTime(&systime);
					//	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
					//	CUtility::Logger()->d("下载频次 start time.[%s]",cTime);
					//}
					////sprintf( szSql, "select sum(下载频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					////临时使用，相对准确的还是从cjfdtotal取
					////sprintf( szSql, "select sum(下载频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					////item.nDownTimes = GetTimes( szSql );
					////GetItemsNumber(item.szCode,m_UnivXzl,item.nDownTimes);
					//if (taskData->nLog)
					//{
					//	GetLocalTime(&systime);
					//	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
					//	CUtility::Logger()->d("下载频次 end time.[%s]",cTime);
					//}
					//iti = m_UnivXzl.find(item.szCode);
					//if (iti != m_UnivXzl.end())
					//{
					//	item.nDownTimes = (*iti).second;
					//}
					//else
					//{
					//	item.nDownTimes = 0;
					//}

					//H指数
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("H指数 start time.[%s]",cTime);
					}
					//sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHIndex = GetHIndex( szSql );
					if (item.nHIndex <= 0)
					{
						CUtility::Logger()->d("H指数为0 sql[%s]",szSql);
						Sleep(100);
						sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
						if (item.nHIndex <= 0)
						{
							CUtility::Logger()->d("H指数仍然为0 sql[%s]",szSql);
						}
					}
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("H指数 end time.[%s]",cTime);
					}

					//核心期刊载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nHXTitles = GetTimes( szSql );
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("核心期刊载文量 start time.[%s]",cTime);
					}
					GetItemsNumber(item.szCode,m_UnivHx,item.nHXTitles);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("核心期刊载文量 end time.[%s]",cTime);
					}

					//SCI载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nSCITitles = GetTimes( szSql );
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("SCI载文量 start time.[%s]",cTime);
					}
					GetItemsNumber(item.szCode,m_UnivSci,item.nSCITitles);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("SCI载文量 end time.[%s]",cTime);
					}

					//EI载文量
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and EI收录刊=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nEITitles = GetTimes( szSql );
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("EI载文量 start time.[%s]",cTime);
					}
					GetItemsNumber(item.szCode,m_UnivEi,item.nEITitles);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("EI载文量 end time.[%s]",cTime);
					}

					//基金论文数
					//sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y", "cjfdtotal", pField, item.szCode, (*it)->szCode );
					//item.nFundTitles = GetTimes( szSql );
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("基金论文数 start time.[%s]",cTime);
					}
					GetItemsNumber(item.szCode,m_UnivFun,item.nFundTitles);
					if (taskData->nLog)
					{
						GetLocalTime(&systime);
						sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
						CUtility::Logger()->d("基金论文数 end time.[%s]",cTime);
					}

					WriteRecFile( hFile, "Code", item.szCode, false );
					WriteRecFile( hFile, "Name", item.szName, true );
					WriteRecFile( hFile, "发文量", item.nTitles, true );
					}
					break;

				case REF_RUNZTCSONLY:	//高被引专题
					{
						//被引频次
						//sprintf( szSql, "select sum(被引频次) from %s where 专题子栏目代码=%s?", "cjfdref", (*it)->szCode );
						//item.nRefTimes = GetTimes( szSql );
						sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where 专题子栏目代码=%s?", "cjfdref", (*it)->szCode );
						GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

						if( item.nRefTimes <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
							continue;
						}
						long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
						item.nTRefRate = ((float)(nFm+5))/1000;
						int nRate = item.nTRefRate*100;
						item.nTRefRate = (float)nRate/100;

						//载文量
						sprintf( szSql, "select count(*) from %s where 专题子栏目代码=%s?", "cjfdtotal", (*it)->szCode );
						item.nTitles = GetTimes( szSql );
						if (item.nTitles == 0)
						{
							CUtility::Logger()->d("执行[%s]时，结果为0",szSql);
							continue;
						}

						//下载量
						//sprintf( szSql, "select sum(下载频次) from %s where 专题子栏目代码=%s?", "cjfdtotal", (*it)->szCode );
						//item.nDownTimes = GetTimes( szSql );

						//H指数
						//sprintf( szSql, "select 被引频次 from %s where 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", (*it)->szCode );
						sprintf( szSql, "select 被引频次 from %s where 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
						WriteRecFile( hFile, "发文量", item.nTitles, false );
					}
					break;
            
				case REF_RUNDOCUONLY:	//高被引文献
					break;

				default:
					break;
			}

			WriteRecFile( hFile, "被引频次", item.nRefTimes, true );
			WriteRecFile( hFile, "下载频次", item.nDownTimes, true );
			WriteRecFile( hFile, "核心期刊", item.nHXTitles, true );
			WriteRecFile( hFile, "SCI收录刊", item.nSCITitles, true );
			WriteRecFile( hFile, "EI收录刊", item.nEITitles, true );
			WriteRecFile( hFile, "基金论文数", item.nFundTitles, true );
			WriteRecFile( hFile, "H指数", item.nHIndex, true );
			WriteRecFile( hFile, "他引频次", item.nTRefTimes, true );
			if (item.nTRefRate < 1.0)
			{
				WriteRecFileFloat( hFile, "他引率", item.nTRefRate, true );
			}
			else
			{
				WriteRecFile( hFile, "他引率", item.nTRefRate, true );
			}
			if( m_ReUpData == 1 )
			{
				WriteRecFile( hFile, "专题代码", (*it)->szCode, true );
				WriteRecFile( hFile, "专题名称", (*it)->szName, true );
			}
			//唯一键值
			strUniqField = (*it)->szCode;
			items.clear();
			CUtility::Str()->CStrToStrings(item.szCode,items,'+');
			if (items.size() == 1)
			{
				strUniqField += items[0];
			}
			if (items.size() > 1)
			{
				strUniqField += items[0];
				strUniqField += items[1];
			}
			WriteRecFile( hFile, "唯一键值", strUniqField.c_str(), true );
			switch(nType)    
			{
				case REF_RUNQIKAONLY:	//高被引期刊
					{
						WriteRecFile( hFile, "Code", item.szCode, true );
						WriteRecFile( hFile, "更新日期", szDate, true );
					}
					break;

				case REF_RUNAUTHONLY:	//高被引作者
				case REF_RUNHOSPONLY:	//高被引医院
				case REF_RUNUNIVONLY:	//高被引院校
				case REF_RUNZTCSONLY:	//高被引专题
						WriteRecFile( hFile, "更新日期", szDate, true );
					break;

				default:
					break;
			}
			++nCountRec;
			if (nCountRec >= GETRECORDNUM)
			{
				nCountRec = GETRECORDNUM;
				break;
			}
		}
		nAllResults += nCountRec;
		nCountRec = 0;
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("one loop end time.[%s]",cTime);
		}
		//++nCount;
		//if (nCount == 2)
		//{
		//	break;
		//}
		::fflush(hFile);
		Sleep(50);
	}

	CloseRecFile( hFile );
	switch(nType)    
	{
		case REF_RUNQIKAONLY:	//高被引期刊
		case REF_RUNAUTHONLY:	//高被引作者
		case REF_RUNUNIVONLY:	//高被引院校
			{
				if (nAllResults != 16800)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
			}
			break;

		case REF_RUNHOSPONLY:	//高被引医院
				if (nAllResults < 2750)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
				break;

		case REF_RUNZTCSONLY:	//高被引专题
				if (nAllResults != 168)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
			break;
            
		default:
			break;
	}

	if( m_ErrCode >= 0 && nRecordCount > 0 )
	{
		nErrCode = nRecordCount;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nRecordCount[%d]",nRecordCount);
	}

	return nErrCode;

}

int CKBConn::StatNewUpdate(DOWN_STATE * taskData, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	char pTable[255] = {0};
	strcpy(pTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, pTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	int iCount = 0;
	ST_ITEM item;
	char szSql[MAX_SQL], szBuff[64];
	vector<ST_ZJCLS *>::iterator it;
	std::map<hfsstring,string>::iterator itt;
	std::map<hfsstring,int>::iterator iti;
	string strZtCode = "";
	string strAuthorCode = "";
	vector<pair<string,int>> tmpVector;  
	vector<pair<string,int>> tVector;  
	vector<string> items;
	string::size_type   pos(0);     
	string strCode = "";
	string strPykm = "";
	string strResult = "";
	string strUniqField = "";
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	char cTime[64] = {0};
	nType = taskData->nStatType;
	for( it =  m_ZJCls.begin(); it !=  m_ZJCls.end(); ++ it )
	{
		CUtility::Logger()->d("doing.[%s][%s]",(*it)->szCode,taskData->tablename);

		memset(szSql,0,sizeof(szSql));
		switch(nType)    
		{
			case REF_RUNQIKAONLY:	//高被引期刊
			case REF_RUNAUTHONLY:	//高被引作者
			case REF_RUNUNIVONLY:	//高被引院校
			case REF_RUNZTCSONLY:	//高被引专题
					sprintf( szSql, "select %s as code,唯一键值 as uniqCode from %s where 专题代码 = %s? ", pField, pTable, (*it)->szCode );
					break;
			case REF_RUNHOSPONLY:	//高被引医院
				if( strchr( (*it)->szCode, 'E' ) )
					sprintf( szSql, "select %s as code,唯一键值 as uniqCode from %s where 专题代码 = %s? ", pField, pTable, (*it)->szCode );
				else
					continue;
				break;
			default:
				break;
		}
		m_vCodeUniqCode.clear();
		LoadFieldInfo(szSql,m_vCodeUniqCode);
		nRecordCount = m_vCodeUniqCode.size();

		nRet = 0;
		if( nRecordCount <= 0 )
		{
			CUtility::Logger()->d("m_vCodeUniqCode的大小为0");
			continue;
			//CloseRecFile( hFile );
			//return 0;
		}

		switch(nType)    
		{
			case REF_RUNQIKAONLY:	//高被引期刊
				break;
			case REF_RUNAUTHONLY:	//高被引作者
				break;
			case REF_RUNHOSPONLY:	//高被引医院
			case REF_RUNUNIVONLY:	//高被引院校
				{
					string strCodeAll = "";
					int nItems = 0;
					for( int i = 0; i < nRecordCount; ++i )
					{
						items.clear();
						CUtility::Str()->CStrToStrings(m_vCodeUniqCode[i].c_str(),items,',');
						nItems = items.size();
						if (nItems > 0)
						{
							strCodeAll += items[0];
							strCodeAll += "+";
						}
					
					}
					nItems = strCodeAll.length();
					if (nItems >= 1)
					{
						strCodeAll = strCodeAll.substr(0,nItems - 1);
					}
					//m_ErrCode = AddUnivItem((*it)->szCode,pTable,strCodeAll,pField2,pCodeName,pDict);
				}
				break;
			default:
				break;
		}

		if( m_ErrCode < 0 )
		{
			CloseRecFile( hFile );
			return m_ErrCode;
		}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		for( int i = 0; i < nRecordCount; i ++ )
		{
			
			memset( &item, 0, sizeof(ST_ITEM) );
			
			items.clear();
			CUtility::Str()->CStrToStrings(m_vCodeUniqCode[i].c_str(),items,',');
			if (items.size() < 2)
			{
				continue;
			}
			//Code
			strcpy(item.szCode,items[0].c_str());
			//Name
			strcpy(item.szUniqCode,items[1].c_str());

			switch(nType)    
			{
				case REF_RUNQIKAONLY:	//高被引期刊
					{
						//sprintf( szSql, "select sum(被引频次) from %s where %s='%s' and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
						//item.nRefTimes = GetTimes( szSql );

						sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
						GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

						if( item.nRefTimes <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
							continue;
						}
						long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
						item.nTRefRate = ((float)(nFm+5))/1000;
						int nRate = item.nTRefRate*100;
						item.nTRefRate = (float)nRate/100;

						strCode = item.szCode;
						if( (pos=strCode.find("_"))!=string::npos )
						{
							strPykm = strCode.substr(pos+1,strCode.length());
						}
						//载文量
						sprintf( szSql, "select count(*) from %s where %s='%s' and 专题子栏目代码=%s?", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						item.nTitles = GetTimes( szSql );
						if( item.nTitles <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
							Sleep(5*1000);
							continue;
						}

						//下载量
						//sprintf( szSql, "select sum(下载频次) from %s where %s='%s' and 专题子栏目代码=%s?", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						//item.nDownTimes = GetTimes( szSql );

						//H指数
						//sprintf( szSql, "select 被引频次 from %s where %s='%s' and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						sprintf( szSql, "select 被引频次 from %s where %s='%s' and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, strPykm.c_str(), (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
					}
					break;

				case REF_RUNAUTHONLY:	//高被引作者
					{
					//被引频次
					//sprintf( szSql, "select sum(被引频次) from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nRefTimes = GetTimes( szSql );

					//他引频次
					//sprintf( szSql, "select sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nTRefTimes = GetTimes( szSql );
					//被引频次、他引频次
					sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

					if( item.nRefTimes <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
						continue;
					}
					long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
					item.nTRefRate = ((float)(nFm+5))/1000;
					int nRate = item.nTRefRate*100;
					item.nTRefRate = (float)nRate/100;

					//载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivZwl,item.nTitles);
					if( item.nTitles <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
						Sleep(5*1000);
						continue;
					}
                    
					//下载量
					//sprintf( szSql, "select sum(下载频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					//item.nDownTimes = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivXzl,item.nDownTimes);

					//H指数
					//sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHIndex = GetHIndex( szSql );
					//核心期刊载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHXTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivHx,item.nHXTitles);
					//SCI载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nSCITitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivSci,item.nSCITitles);
					//EI载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and EI收录刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nEITitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivEi,item.nEITitles);
					//基金论文数
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nFundTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivFun,item.nFundTitles);

					}
					break;

				case REF_RUNHOSPONLY:	//高被引医院
				case REF_RUNUNIVONLY:	//高被引院校
					{
					//被引频次
					//sprintf( szSql, "select sum(被引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					//item.nRefTimes = GetTimes( szSql );

					//被引频次、他引频次
					sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdref", pField2, item.szCode, (*it)->szCode );
					GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );
					if (item.nRefTimes == 0)
					{
						CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
						continue;
					}
					long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
					item.nTRefRate = ((float)(nFm+5))/1000;
					int nRate = item.nTRefRate*100;
					item.nTRefRate = (float)nRate/100;

					//载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s?", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivZwl,item.nTitles);
					if( item.nTitles <= 0 )
					{
						CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
						Sleep(5*1000);
						continue;
					}

					//H指数
					//sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHIndex = GetHIndex( szSql );
					if (item.nHIndex <= 0)
					{
						CUtility::Logger()->d("H指数为0 sql[%s]",szSql);
						Sleep(100);
						sprintf( szSql, "select 被引频次 from %s where %s=%s and 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
						if (item.nHIndex <= 0)
						{
							CUtility::Logger()->d("H指数仍然为0 sql[%s]",szSql);
						}
					}

					//核心期刊载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nHXTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivHx,item.nHXTitles);

					//SCI载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nSCITitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivSci,item.nSCITitles);

					//EI载文量
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and EI收录刊=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nEITitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivEi,item.nEITitles);

					//基金论文数
					sprintf( szSql, "select count(*) from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y", "cjfdtotal", pField2, item.szCode, (*it)->szCode );
					item.nFundTitles = GetTimes( szSql );
					//GetItemsNumber(item.szCode,m_UnivFun,item.nFundTitles);
					}
					break;

				case REF_RUNZTCSONLY:	//高被引专题
					{
						//被引频次
						sprintf( szSql, "select sum(被引频次),sum(他引频次) from %s where 专题子栏目代码=%s?", "cjfdref", (*it)->szCode );
						GetTimes2( szSql,item.nRefTimes,item.nTRefTimes );

						if( item.nRefTimes <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，被引频次为0",szSql);
							continue;
						}
						long long nFm = (long long)(item.nTRefTimes*1000/item.nRefTimes);
						item.nTRefRate = ((float)(nFm+5))/1000;
						int nRate = item.nTRefRate*100;
						item.nTRefRate = (float)nRate/100;

						//载文量
						sprintf( szSql, "select count(*) from %s where 专题子栏目代码=%s?", "cjfdtotal", (*it)->szCode );
						item.nTitles = GetTimes( szSql );
						if( item.nTitles <= 0 )
						{
							CUtility::Logger()->d("执行[%s]时，载文量为0",szSql);
							Sleep(5*1000);
							continue;
						}

						//下载量
						//sprintf( szSql, "select sum(下载频次) from %s where 专题子栏目代码=%s?", "cjfdtotal", (*it)->szCode );
						//item.nDownTimes = GetTimes( szSql );

						//H指数
						//sprintf( szSql, "select 被引频次 from %s where 专题子栏目代码=%s? not 引证标识=0 order by (被引频次,integer) desc", "cjfdtotal", (*it)->szCode );
						sprintf( szSql, "select 被引频次 from %s where 专题子栏目代码=%s? order by (被引频次,integer) desc", "cjfdtotal", (*it)->szCode );
						item.nHIndex = GetHIndex( szSql );
					}
					break;
            
				default:
					break;
			}
			//唯一键值
			WriteRecFile( hFile, "唯一键值", item.szUniqCode, false );
			WriteRecFile( hFile, "发文量", item.nTitles, true );
			WriteRecFile( hFile, "被引频次", item.nRefTimes, true );
			WriteRecFile( hFile, "下载频次", item.nDownTimes, true );
			WriteRecFile( hFile, "核心期刊", item.nHXTitles, true );
			WriteRecFile( hFile, "SCI收录刊", item.nSCITitles, true );
			WriteRecFile( hFile, "EI收录刊", item.nEITitles, true );
			WriteRecFile( hFile, "基金论文数", item.nFundTitles, true );
			WriteRecFile( hFile, "H指数", item.nHIndex, true );
			WriteRecFile( hFile, "他引频次", item.nTRefTimes, true );
			if (item.nTRefRate < 1.0)
			{
				WriteRecFileFloat( hFile, "他引率", item.nTRefRate, true );
			}
			else
			{
				WriteRecFile( hFile, "他引率", item.nTRefRate, true );
			}
			if( m_ReUpData == 1 )
			{
				WriteRecFile( hFile, "专题代码", (*it)->szCode, true );
				WriteRecFile( hFile, "专题名称", (*it)->szName, true );
			}
			switch(nType)    
			{
				case REF_RUNQIKAONLY:	//高被引期刊
					{
						WriteRecFile( hFile, "Code", item.szCode, true );
						WriteRecFile( hFile, "更新日期", szDate, true );
						//自社科
						sprintf( szSql, "select top 1 自社科 as result from %s where %s='%s'", "cjfdref", pField2, item.szCode );
						GetResult( szSql,strResult );
						WriteRecFile( hFile, "自社科", strResult.c_str(), true );
					}
					break;

				case REF_RUNAUTHONLY:	//高被引作者
				case REF_RUNHOSPONLY:	//高被引医院
				case REF_RUNUNIVONLY:	//高被引院校
				case REF_RUNZTCSONLY:	//高被引专题
						WriteRecFile( hFile, "更新日期", szDate, true );
					break;

				default:
					break;
			}
			++nCountRec;
			//if (nCountRec >= GETRECORDNUM)
			//{
			//	nCountRec = GETRECORDNUM;
			//	break;
			//}
		}
		nAllResults += nCountRec;
		nCountRec = 0;
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("one loop end time.[%s]",cTime);
		}
		//++nCount;
		//if (nCount == 2)
		//{
		//	break;
		//}
		::fflush(hFile);
		Sleep(50);
	}

	CloseRecFile( hFile );
	switch(nType)    
	{
		case REF_RUNQIKAONLY:	//高被引期刊
		case REF_RUNAUTHONLY:	//高被引作者
		case REF_RUNUNIVONLY:	//高被引院校
			{
				if (nAllResults != 16800)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
			}
			break;

		case REF_RUNHOSPONLY:	//高被引医院
				if (nAllResults !=2800)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
				break;

		case REF_RUNZTCSONLY:	//高被引专题
				if (nAllResults != 168)
				{
					CUtility::Logger()->d("数据中出现异常，请查看[%d]",nAllResults);
					nRecordCount = 0;
					nErrCode = 0;
				}
			break;
            
		default:
			break;
	}

	if( m_ErrCode >= 0 && nAllResults > 0 )
	{
		nErrCode = nAllResults;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nAllResults[%d]",nAllResults);
	}

	return nErrCode;

}

int	CKBConn::AddMapAuthorCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable)
{
	hfschar t[64] = {0};
	hfsstring strName = "";
	hfsstring strAuthorCode = "";
	hfsstring strCitedNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	std::map<hfsstring,int>::iterator it;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Code",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		//Name
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Name",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strName = t;
		}

		if( !IsValidata( t, pTable ) )
		{
			TPI_MoveNext( hSet );
			continue;
		}
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Counts",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			nCitedNum = (int)_atoi64(t);
		}
		if (strAuthorCode == strName)
		{
			CUtility::Logger()->d("数据需要处理,作者名称groupcodename(sys_authorcodename)没有正确获得:作者代码[%s]",strAuthorCode.c_str());
		}
		strFist = strAuthorCode;
		strFist += ";";
		strFist += strName;
		strFist += ";";
		it = map.find(strFist);
		if(it == map.end()){
			map.insert(make_pair(strFist,nCitedNum));
		}
		else
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCount ++;
		if( nCount >= GETRECORDNUM*2 )
			break;

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int	CKBConn::AddMapAuthorCitedNumMulti(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable)
{
	hfschar t[64] = {0};
	hfsstring strName = "";
	hfsstring strAuthorAllCode = "";
	hfsstring strAuthorCode = "";
	hfsstring strCitedNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	int nAuthorCount = 0;
	std::map<hfsstring,string>::iterator it;
	std::map<hfsstring,int>::iterator itt;
	vector<string> authors;
	vector<string> items;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"allcode",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorAllCode = t;
		}
		authors.clear();
		CUtility::Str()->CStrToStrings(strAuthorAllCode.c_str(),authors,';');
		nAuthorCount = authors.size();
		if (nAuthorCount < 2)
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCitedNum = 0;
		for (int i=0;i < nAuthorCount;++i)
		{
			it = m_AuthorCodeCitedNumMiddle.find(authors[i]);
			items.clear();
			if (it != m_AuthorCodeCitedNumMiddle.end())
			{
				CUtility::Str()->CStrToStrings((*it).second.c_str(),items,';');
			}

			if (items.size() > 0)
			{
				strName = items[0];
				nCitedNum += (int)_atoi64(items[1].c_str());
			}
			if (i == 0)
			{
				strFist = authors[0];
			}
			else
			{
				strFist += "+";
				strFist += authors[i];
			}
		}
		if (nCitedNum < 1)
		{
			TPI_MoveNext(hSet);
			continue;
		}
		strFist += ";";
		strFist += strName;
		strFist += ";";
		itt = map.find(strFist);
		if(itt == map.end()){
			map.insert(make_pair(strFist,nCitedNum));
		}
		else
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCount ++;
		if( nCount >= GETRECORDNUM )
			break;

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int	CKBConn::AddMapUnivCitedNumMulti(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable)
{
	hfschar t[2048] = {0};
	hfsstring strName = "";
	hfsstring strAuthorAllCode = "";
	hfsstring strAuthorCode = "";
	hfsstring strCitedNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	int nAuthorCount = 0;
	int nLen = 0;
	nLen = (int)sizeof(t);
	std::map<hfsstring,string>::iterator it;
	std::map<hfsstring,int>::iterator itt;
	vector<string> authors;
	vector<string> items;
	bool bGetName = false;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,nLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"code",t,nLen);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		memset(t,0,nLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"name",t,nLen);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strName = t;
		}
		memset(t,0,nLen);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"allcode",t,nLen);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorAllCode = t;
		}
		authors.clear();
		CUtility::Str()->CStrToStrings(strAuthorAllCode.c_str(),authors,'+');
		nAuthorCount = authors.size();
		if (nAuthorCount < 2)
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCitedNum = 0;
		for (int i=0;i < nAuthorCount;++i)
		{
			it = m_AuthorCodeCitedNumMiddle.find(authors[i]);
			items.clear();
			if (it != m_AuthorCodeCitedNumMiddle.end())
			{
				CUtility::Str()->CStrToStrings((*it).second.c_str(),items,';');
			}

			if (items.size() > 0)
			{
				nCitedNum += (int)_atoi64(items[1].c_str());
			}
		}
		if (nCitedNum < 1)
		{
			TPI_MoveNext(hSet);
			continue;
		}
		strFist = strAuthorAllCode;
		strFist += ";";
		strFist += strName;
		strFist += ";";
		itt = map.find(strFist);
		if(itt == map.end()){
			map.insert(make_pair(strFist,nCitedNum));
		}
		else
		{
			(*itt).second = nCitedNum;
			TPI_MoveNext(hSet);
			continue;
		}
		//nCount ++;
		//if( nCount >= GETRECORDNUM )
		//	break;

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int cmp(const pair<string,int>& x,const pair<string,int>& y)
{
	return x.second>y.second;
}
void sortMapByValue(map<string,int>& tMap,vector<pair<string,int>>& tVector)
{
	for(map<string,int>::iterator curr=tMap.begin();curr!=tMap.end();curr++)
	{
		tVector.push_back(make_pair(curr->first,curr->second));
	}
	sort(tVector.begin(),tVector.end(),cmp);
}

bool CKBConn::IsZeroSize(const char *pRecFile)
{
	int handle;
	handle = open(pRecFile, 0x0100); //open file for read
	long length = filelength(handle); //get length of file
	close(handle);
	if (length == 0)
	{
		return true;
	}
	return false;
}

int CKBConn::UpdateTable(char *pTable, const char *pRecFile)
{
	if (IsZeroSize(pRecFile))
	{
		return 0;
	}

	char szSql[1024];
	sprintf( szSql, "dbum activate table %s false", pTable );
	int nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;
	
	sprintf( szSql, "CLEAR TABLE %s", pTable );
	nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "PACK TABLE %s", pTable );
	nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "bulkload table %s '%s'", pTable, pRecFile);
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "index %s all from 0", pTable );
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "dbum activate table %s true", pTable );
	nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;

	return 0;
}

int CKBConn::UpdateTableTrue(char *pTable, const char *pRecFile, const char *pKey)
{
	if (IsZeroSize(pRecFile))
	{
		return 0;
	}
	int nRet = 0;
	char szSql[1024];
	sprintf( szSql, "DBUM UPDATE table %s FROM '%s' WITH KEY %s", pTable, pRecFile, pKey);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	//sprintf( szSql, "dbum refresh sortfile of table %s", pTable );
	//nRet = ExecMgrSql( szSql );
	//if( nRet < 0 )
	//	return nRet;

	return 0;
}

int CKBConn::UpdateFiledValue(char *pTable)
{
	int nRet = 0;
	char szSql[1024];
	sprintf( szSql, "UPDATE %s set 一季度下载=一月下载+二月下载+三月下载", pTable);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 二季度下载=四月下载+五月下载+六月下载", pTable);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 三季度下载=七月下载+八月下载+九月下载", pTable);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 四季度下载=十月下载+十一月下载+十二月下载", pTable);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 年下载=一季度下载+二季度下载+三季度下载+四季度下载", pTable);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	return 0;
}

int CKBConn::UpdateFiledValueYear(char *pTable,int nYear)
{
	int nRet = 0;
	char szSql[1024];
	sprintf( szSql, "UPDATE %s set 一季度下载=一月下载+二月下载+三月下载 where 年=%d", pTable, nYear);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 二季度下载=四月下载+五月下载+六月下载 where 年=%d", pTable, nYear);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 三季度下载=七月下载+八月下载+九月下载 where 年=%d", pTable, nYear);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 四季度下载=十月下载+十一月下载+十二月下载 where 年=%d", pTable, nYear);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;
	sprintf( szSql, "UPDATE %s set 年下载=一季度下载+二季度下载+三季度下载+四季度下载 where 年=%d", pTable, nYear);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	return 0;
}

int	CKBConn::SplitRec(map<hfsstring,int> &map, const char *pRecFile, const char *pRecFileAdd, const char *pRecFileUpdate)
{
	return 0;
}

int CKBConn::GetMap(char *pTable, const char *pKey, map<hfsstring,int> &map, int nyear)
{
	int nRet = 0;
	int nCount = 0;
	char szSql[1024];
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20];
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFile[MAX_PATH];
	char szFile01[MAX_PATH];
	char szFile02[MAX_PATH];
	char szFile03[MAX_PATH];
	GetModuleFileName( NULL, szFile, sizeof(szFile) );
	strcpy(szFile01,szFile); 
	strcpy(szFile02,szFile); 
	strcpy(szFile03,szFile); 
	strcpy( strrchr(szFile, '\\') + 1, pTable );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	strcpy( strrchr(szFile01, '\\') + 1, pTable );
	strcat( szFile01, szDate );
	strcat( szFile01, "_01" );
	strcat( szFile01, ".txt" );
	strcpy( strrchr(szFile02, '\\') + 1, pTable );
	strcat( szFile02, szDate );
	strcat( szFile02, "_02" );
	strcat( szFile02, ".txt" );
	strcpy( strrchr(szFile03, '\\') + 1, pTable );
	strcat( szFile03, szDate );
	strcat( szFile03, "_03" );
	strcat( szFile03, ".txt" );
	//strcpy( strrchr(szFileTwo, '\\') + 1, pTable );
	//strcat( szFileTwo, szDate );
	//strcat( szFileTwo, "2" );
	//strcat( szFileTwo, ".txt" );
	::DeleteFile( szFile );
	::DeleteFile( szFile01 );
	::DeleteFile( szFile02 );
	::DeleteFile( szFile03 );
	//::DeleteFile( szFileTwo );
	sprintf( szSql, "select %s from %s where 年=%d into '%s'", pKey, pTable, nyear, szFile);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
	{
		CUtility::Logger()->d("exec [%s] result[%d]", szSql, nRet);
		return nRet;
	}
	Sleep(500);
	FILE *hFileRec = fopen(szFile,"r"); 
	if (!hFileRec)
	{
		CUtility::Logger()->d("file [%s] not exist", szFile);
		return 0;
	}

	string strLine = "";
	char line[255] = {0};
	vector<string> items;
	int i = 0;
	bool bInsert = false;
	char cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						map.insert(make_pair(items[1],1));
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
		cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}

	hFileRec = fopen(szFile01,"r"); 
	if (!hFileRec)
	{
		CUtility::Logger()->d("file [%s] not exist", szFile01);
		return 0;
	}

	strLine = "";
	memset(line,0,sizeof(line));
	items.clear();
	bInsert = false;
	cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						map.insert(make_pair(items[1],1));
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}

	hFileRec = fopen(szFile02,"r"); 
	if (!hFileRec)
	{
		CUtility::Logger()->d("file [%s] not exist", szFile02);
		return 0;
	}

	strLine = "";
	memset(line,0,sizeof(line));
	items.clear();
	bInsert = false;
	cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						map.insert(make_pair(items[1],1));
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}

	hFileRec = fopen(szFile03,"r"); 
	if (!hFileRec)
	{
		CUtility::Logger()->d("file [%s] not exist", szFile03);
		return 0;
	}

	strLine = "";
	memset(line,0,sizeof(line));
	items.clear();
	bInsert = false;
	cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						map.insert(make_pair(items[1],1));
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}
	//string strFileidOrOne = "";
	//int nRet = 0;
	//int nRecordCount = 0;
	//char szSql[1024];
	//sprintf( szSql, "select %s FROM %s where 年=%d", pKey, pTable, nyear);
	//m_ErrCode = IsConnected();
	//TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	//while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	//{
	//	CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
	//	Sleep( 1000 );
	//	hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	//}
	//if( !hSet || m_ErrCode < 0 )
	//{
	//	char szMsg[MAX_SQL];
	//	sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
	//	CUtility::Logger()->d("error info.[%s]",szMsg);
	//	return m_ErrCode;
	//}
	//nRecordCount = TPI_GetRecordSetCount( hSet );
	//m_ErrCode = TPI_SetRecordCount( hSet, 5000);
	//TPI_MoveFirst( hSet );

	//std::map<hfsstring,int>::iterator it;
	//char t[64];
	//int i = 0;
	//while(!TPI_IsEOF(hSet)){
	//	memset(t,0,64);
	//	m_ErrCode = TPI_GetFieldValue(hSet,0,t,64);	
	//	if(t[0] == '\0'){
	//		TPI_MoveNext(hSet);
	//		continue;
	//	}
	//	else
	//	{
	//		strFileidOrOne = t;
	//	}

	//	it = map.find(strFileidOrOne);
	//	if(it == map.end()){
	//		map.insert(make_pair(strFileidOrOne,1));
	//	}
	//	++i;
	//	if (i%100000 == 0)
	//	{
	//		CUtility::Logger()->d("map size [%d]",i);
	//	}
	//	TPI_MoveNext(hSet);
	//}
	return 0;
}

int	CKBConn::ExecFieldUpdate(DOWN_STATE * taskData)
{
	int nRet = -1;
	nRet = UpdateFiledValue( taskData->cjfdtablename );
	CUtility::Logger()->d("update table [%s] fileid result[%d].",taskData->cjfdtablename,nRet);
	nRet = UpdateFiledValue( taskData->cdmdtablename );
	CUtility::Logger()->d("update table [%s] fileid result[%d].",taskData->cdmdtablename,nRet);
	nRet = UpdateFiledValue( taskData->cpmdtablename );
	CUtility::Logger()->d("update table [%s] fileid result[%d].",taskData->cpmdtablename,nRet);
	return 0;
}

int CKBConn::ExecTextToKbase(DOWN_STATE * taskData)
{
	int nRet = -1;
	string strUniqCode="";
	string strCjfdFileUpdate="E:\\WORK\\DLC\\2013CJFD\\CJFDUPDATE2013";
	string strCdmdFileUpdate="E:\\WORK\\DLC\\2013Cdmd\\cdmdUPDATE2013";
	string strCpmdFileUpdate="E:\\WORK\\DLC\\2013Cpmd\\CpmdUPDATE2013";
	string cjfdtablename = "DOWNCJFD2013";
	string cdmdtablename = "DOWNCDMD0514";
	string cpmdtablename = "DOWNCPMD0514";
	char szcjfdtablename[255];
	char szcdmdtablename[255];
	char szcpmdtablename[255];
	memset(szcjfdtablename,0,sizeof(szcjfdtablename));
	memset(szcdmdtablename,0,sizeof(szcdmdtablename));
	memset(szcpmdtablename,0,sizeof(szcpmdtablename));
	strcpy(szcjfdtablename,cjfdtablename.c_str());
	strcpy(szcdmdtablename,cdmdtablename.c_str());
	strcpy(szcpmdtablename,cpmdtablename.c_str());
	string strDate="2015-05-16.txt";
	//for (int i = 8; i < 12; ++i)
	//{
	//	strCjfdFileUpdate="E:\\WORK\\DLC\\2013CJFD\\CJFDUPDATE2013";
	//	strCjfdFileUpdate = strCjfdFileUpdate + gMonth[i];
	//	strCjfdFileUpdate += strDate;
	//	strUniqCode = "文件名";
	//	CUtility::Logger()->d("update table [%s][%s] start.",cjfdtablename.c_str(),strCjfdFileUpdate.c_str());
	//	nRet = UpdateTableTrue( szcjfdtablename, strCjfdFileUpdate.c_str(), strUniqCode.c_str() );
	//	CUtility::Logger()->d("update table [%s][%s] end[%d].",cjfdtablename.c_str(),strCjfdFileUpdate.c_str(),nRet);
	//}

	for (int i = 1; i < 12; ++i)
	{
		strCdmdFileUpdate="E:\\WORK\\DLC\\2013Cdmd\\cdmdUPDATE2013";
		strCpmdFileUpdate="E:\\WORK\\DLC\\2013Cpmd\\CpmdUPDATE2013";
		strCdmdFileUpdate = strCdmdFileUpdate + gMonth[i];
		strCdmdFileUpdate += strDate;
		strCpmdFileUpdate = strCpmdFileUpdate + gMonth[i];
		strCpmdFileUpdate += strDate;
		strUniqCode = "文件名年";
		//CUtility::Logger()->d("update table [%s][%s] start.",szcdmdtablename,strCdmdFileUpdate.c_str());
		//nRet = UpdateTableTrue( szcdmdtablename, strCdmdFileUpdate.c_str(), strUniqCode.c_str() );
		//CUtility::Logger()->d("update table [%s][%s] end[%d].",szcdmdtablename,strCdmdFileUpdate.c_str(),nRet);
		CUtility::Logger()->d("update table [%s][%s] start.",szcpmdtablename,strCpmdFileUpdate.c_str());
		nRet = UpdateTableTrue( szcpmdtablename, strCpmdFileUpdate.c_str(), strUniqCode.c_str() );
		CUtility::Logger()->d("update table [%s][%s] end[%d].",szcpmdtablename,strCpmdFileUpdate.c_str(),nRet);
	}
	return nRet;
}
int CKBConn::TextToSql()
{
	int nRet = 0;
	int nCount = 0;
	char szSql[1024];
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20];
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFile[MAX_PATH];
	char szFileTwo[MAX_PATH];
	char szFile3[MAX_PATH];
	char szFile4[MAX_PATH];
	GetModuleFileName( NULL, szFile, sizeof(szFile) );
	strcpy(szFileTwo,"E:\\DownCjfd2012filename.txt"); 
	strcpy(szFile3,"E:\\DownCjfd2012filename_01.txt"); 
	strcpy(szFile4,"E:\\DownCjfd2012filename_02.txt"); 
	strcpy( strrchr(szFile, '\\') + 1, "sql" );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	::DeleteFile( szFile );

	FILE *hFileRecW = fopen(szFile,"wb"); 
	if (!hFileRecW)
	{
		return 0;
	}
	FILE *hFileRec = fopen(szFileTwo,"r"); 
	if (!hFileRec)
	{
		return 0;
	}
	string strLine = "";
	char line[255] = {0};
	vector<string> items;
	int i = 0;
	bool bInsert = false;
	char cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						fwrite(items[1].c_str(),1,(int)strlen(items[1].c_str()),hFileRecW);
						fwrite( "\n\r", 1, 2, hFileRecW );
						::fflush(hFileRecW);
					}
				}
			}
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}

	hFileRec = fopen(szFile3,"r"); 
	if (!hFileRec)
	{
		return 0;
	}
	strLine = "";
	bInsert = false;
	cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						fwrite(items[1].c_str(),1,(int)strlen(items[1].c_str()),hFileRecW);
						fwrite( "\n\r", 1, 2, hFileRecW );
						::fflush(hFileRecW);
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}

	hFileRec = fopen(szFile4,"r"); 
	if (!hFileRec)
	{
		return 0;
	}
	strLine = "";
	bInsert = false;
	cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				if (items.size()==2)
				{
					if ( items[1]!="")
					{
						fwrite(items[1].c_str(),1,(int)strlen(items[1].c_str()),hFileRecW);
						fwrite( "\n\r", 1, 2, hFileRecW );
						::fflush(hFileRecW);
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read map line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
        cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}
	if(hFileRecW)
	{
		fclose(hFileRecW);
	}

	return 0;
}

int CKBConn::AppendTable(char *pTable, const char *pRecFile)
{
	if (IsZeroSize(pRecFile))
	{
		return 0;
	}

	char szSql[1024];
	int nRet = 0;
	sprintf( szSql, "bulkload table %s '%s'", pTable, pRecFile);
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
	{
		return nRet;
	}

	sprintf( szSql, "index %s all from 0", pTable );
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
	{
		return nRet;
	}

	return 0;
}

int CKBConn::SortTable(int nType)
{
	char szSql[1024];
	int nRet = 0;
	switch(nType)    
	{
		case REF_RUNQIKAONLY:	//高被引期刊
			{
				sprintf( szSql, "dbum make sortcol by integer(qika_ref.发文量)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(qika_ref.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(qika_ref.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(qika_ref.H指数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum refresh sortfile of table qika_ref");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
			}
			break;
		case REF_RUNAUTHONLY:	//高被引作者
			{
				sprintf( szSql, "dbum make sortcol by integer(auth_ref.发文量)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(auth_ref.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.核心期刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.基金论文数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.EI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.SCI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.H指数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum make sortcol by integer(auth_ref.他引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;

				sprintf( szSql, "dbum refresh sortfile of table auth_ref");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
			}
			break;
		case REF_RUNUNIVONLY:	//高被引院校
			{
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.发文量)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.核心期刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.基金论文数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.EI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.SCI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(UNIV_REF.H指数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum refresh sortfile of table UNIV_REF");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
 			}
			break;

		case REF_RUNHOSPONLY:	//高被引医院
			{
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.发文量)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.核心期刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.基金论文数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.EI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.SCI收录刊)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(HOSP_REF.H指数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum refresh sortfile of table HOSP_REF");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
			}
			break;

		case REF_RUNZTCSONLY:	//高被引专题
			{
				sprintf( szSql, "dbum make sortcol by integer(ZTCS_REF.发文量)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(ZTCS_REF.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(ZTCS_REF.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(ZTCS_REF.H指数)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum refresh sortfile of table ZTCS_REF");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
			}
			break;
            
		case REF_RUNDOCUONLY:	//高被引文献
			{
				sprintf( szSql, "dbum make sortcol by integer(DOCU_REF.被引频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum make sortcol by integer(DOCU_REF.下载频次)");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
				
				sprintf( szSql, "dbum refresh sortfile of table DOCU_REF");
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
					return nRet;
			}
			break;

		default:
			break;
	}
	/*
	
	sprintf( szSql, "CLEAR TABLE %s", pTable );
	nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "bulkload table %s '%s'", pTable, pRecFile);
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "index %s all from 0", pTable );
	//CUtility::Logger()->d("exec sql .[%s]",szSql);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
		return nRet;

	sprintf( szSql, "dbum activate table %s true", pTable );
	nRet = ExecSql( szSql );
	if( nRet < 0 )
		return nRet;
	*/
	return 0;
}

int CKBConn::ExecSql(char *pSql)
{
	int nRet = IsConnected();
	if( nRet < 0 )
	{
		char szMsg[512];
		sprintf( szMsg, "IsConnected(IP=%s, Port=%d) failed, ErrCode=%d!", m_szIP, m_nPort, nRet );
		CUtility::Logger()->d("exec sql .[%s]",szMsg);

		return nRet;
	}

	nRet = TPI_ExecSQL( m_hConn, pSql );
	while( nRet == TPI_ERR_CMDTIMEOUT || nRet == TPI_ERR_BUSY )
	{
		Sleep( 1000 );
		nRet = TPI_ExecSQL( m_hConn, pSql );
	}

	return nRet;
}

int CKBConn::ExecMgrSql(char *pSql, bool bWait)
{
	int nRet = IsConnected();
	if( nRet < 0 )
	{
		char szMsg[512];
		sprintf( szMsg, "IsConnected(IP=%s, Port=%d) failed, ErrCode=%d!", m_szIP, m_nPort, nRet );
		CUtility::Logger()->d("exec sql .[%s]",szMsg);

		return nRet;
	}

	TPI_HEVENT  hTmpEvent = NULL;
	TPI_HEVENT *pEventHandle = &hTmpEvent;
	for( int i = 0; i < 5; i ++ )
	{
		if( bWait )
			nRet = TPI_ExecMgrSql( m_hConn, pSql, pEventHandle );
		else
			nRet = TPI_ExecMgrSql( m_hConn, pSql );

		if( nRet == TPI_ERR_INVALID_UID )
			IsConnected();
		else if( nRet != TPI_ERR_DOSQLERROR )
			break;
	}
	if( bWait && nRet >= 0 )
	{
		nRet = TPI_QueryEvent( m_hConn, *pEventHandle );
		while( nRet == TPI_ERR_EVENTNOEND || nRet == TPI_ERR_CMDTIMEOUT || nRet == TPI_ERR_BUSY || nRet == TPI_ERR_INVALID_UID )
		{
			Sleep( 1000 );

			if( nRet == TPI_ERR_INVALID_UID )
				IsConnected();
			nRet = TPI_QueryEvent( m_hConn, *pEventHandle );
		}
	}
	else
	{
		char szMsg[512];
		sprintf( szMsg, "CKBConn::ExecMgrSql() error, ErrCode=%d", nRet );
		CUtility::Logger()->d("exec sql .[%s]",szMsg);
	}

	return nRet;
}

int	CKBConn::AddMapAuthorCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,string> &map,hfsint iCountNum,char * pTable)
{
	hfschar t[64] = {0};
	hfsstring strName = "";
	hfsstring strAuthorCode = "";
	hfsstring strCitedNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	std::map<hfsstring,string>::iterator it;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Code",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		//Name
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Name",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strName = t;
		}

		if( !IsValidata( t, pTable ) )
		{
			TPI_MoveNext( hSet );
			continue;
		}
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Counts",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCitedNum = t;
			nCitedNum = (int)_atoi64(t);
		}
		strFist = strName;
		strFist += ";";
		strFist += strCitedNum;
		strFist += ";";
		it = map.find(strAuthorCode);
		if(it == map.end()){
			map.insert(make_pair(strAuthorCode,strFist));
		}
		else
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCount ++;
		if( nCount >= GETRECORDNUM )
			break;

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int	CKBConn::AddMapUnivCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,string> &map,hfsint iCountNum,char * pTable,std::map<hfsstring,int> &Curmap)
{
	hfschar t[64] = {0};
	hfsstring strName = "";
	hfsstring strAuthorCode = "";
	hfsstring strCitedNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	std::map<hfsstring,string>::iterator it;
	std::map<hfsstring,int>::iterator it2;
	for (int i = 1;i <= iCountNum;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Code",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strAuthorCode = t;
		}
		it2 = Curmap.find(strAuthorCode);
		if (it2 == Curmap.end())
		{
			continue;
		}
		//Name
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Name",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strName = t;
		}

		if( !IsValidata( t, pTable ) )
		{
			TPI_MoveNext( hSet );
			continue;
		}
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"Counts",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCitedNum = t;
			nCitedNum = (int)_atoi64(t);
		}
		strFist = strName;
		strFist += ";";
		strFist += strCitedNum;
		strFist += ";";
		it = map.find(strAuthorCode);
		if(it == map.end()){
			map.insert(make_pair(strAuthorCode,strFist));
		}
		else
		{
			TPI_MoveNext(hSet);
			continue;
		}
		nCount ++;
		if( nCount >= GETRECORDNUM )
			break;

		TPI_MoveNext(hSet);
	}
	return 1;		
}

int	CKBConn::AddAuthorMulti(char * pZtCode, char * pTable, char *pWhere, char *pField, char *pCodeName, char *pDict)
{
	map<hfsstring,hfsstring>::iterator itt;
	string strAuthorCode = "";
	char szSql[MAX_SQL]={0};
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	string strZtCode = pZtCode;
	itt = (m_ZtCodeAuthorCode.find(strZtCode));
	string strUnivCode = "";
	vector<string> items;
	m_CurUnivCode.clear();
	if (itt != m_ZtCodeAuthorCode.end())
	{
		items.clear();
		strAuthorCode = itt->second;
		CUtility::Str()->CStrToStrings(strAuthorCode.c_str(),items,'+');
		int iCount = items.size();
		for (int i = 0;i < iCount;++i)
		{
			strUnivCode = items[i];
			m_CurUnivCode.insert(make_pair(strUnivCode,0));
		}
		if( m_ReUpData == 0 )
			sprintf( szSql, "select Code, Name from %s where  专题代码=%s", pTable, pZtCode );
		else
		{
			if( pWhere == NULL )
			{
				sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s = %s group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", pZtCode, pField, strAuthorCode.c_str(), pField, pDict );
				//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s = %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", pZtCode, pField, strAuthorCode.c_str(), pField, pDict );
			}
			else
			{
				sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s and %s = %s  group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", pZtCode, pWhere, pField, strAuthorCode.c_str(), pField, pDict );
				//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s and %s = %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", pZtCode, pWhere, pField, strAuthorCode.c_str(), pField, pDict );
			}
		}
		m_ErrCode = IsConnected();
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			if( m_ErrCode >= 0 )
				m_ErrCode = -1;

			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		TPI_MoveFirst( hSet );
		m_AuthorCodeCitedNumMiddle.clear();
		//AddMapAuthorCitedNum(hSet,m_AuthorCodeCitedNumMiddle,nRecordCount,pTable);
		AddMapUnivCitedNum(hSet,m_AuthorCodeCitedNumMiddle,nRecordCount,pTable,m_CurUnivCode);
		m_CurUnivCode.clear();
		TPI_CloseRecordSet( hSet );
	}


	sprintf( szSql, "SELECT 作者所有代码 as allcode,作者代码 as code from AUTH_CHECK where 专题代码=%s GROUP BY 作者所有代码",pZtCode );
	m_ErrCode = IsConnected();
	hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
	TPI_MoveFirst( hSet );
	AddMapAuthorCitedNumMulti(hSet,m_AuthorCodeCitedNum,nRecordCount,pTable);
	TPI_CloseRecordSet( hSet );
	return m_ErrCode;
}

int	CKBConn::AddUnivMulti(char * pZtCode, char * pTable, char *pWhere, char *pField, char *pCodeName, char *pDict, char *pSourceTable)
{
	map<int,hfsstring>::iterator itt;
	string strAuthorCode = "";
	string strUnivCode = "";
	char szSql[MAX_SQL]={0};
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	string strZtCode = pZtCode;
	m_AuthorCodeCitedNumMiddle.clear();
	vector<string> items;
	m_CurUnivCode.clear();
	for (itt = m_UnivCodeMulti.begin();itt != m_UnivCodeMulti.end();++itt)
	{
		items.clear();
		strAuthorCode = itt->second;
		CUtility::Str()->CStrToStrings(strAuthorCode.c_str(),items,'+');
		int iCount = items.size();
		for (int i = 0;i < iCount;++i)
		{
			strUnivCode = items[i];
			m_CurUnivCode.insert(make_pair(strUnivCode,0));
		}

		if( m_ReUpData == 0 )
			sprintf( szSql, "select Code, Name from %s where  专题代码=%s", pTable, pZtCode );
		else
		{
			if( pWhere == NULL )
			{
				sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s = %s group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", pZtCode,pField, strAuthorCode.c_str(), pField, pDict );
				//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s = %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", pZtCode,pField, strAuthorCode.c_str(), pField, pDict );
			}
			else
			{
				sprintf( szSql, "select %s as Code,%s as Name,sum(被引频次) as Counts from %s where 专题子栏目代码=%s? and %s and %s = %s group by (%s,%s) order by sum(被引频次) desc", pField, pCodeName, "cjfdref", pZtCode, pWhere, pField, strAuthorCode.c_str(),  pField, pDict );
				//sprintf( szSql, "select %s as Code,%s as Name,count(*) as Counts from %s where 被引专题子栏目代码=%s? and %s = %s group by (%s,%s) order by count(*) desc", pField, pCodeName, "cojdref", pZtCode, pField, strAuthorCode.c_str(), pField, pDict );
			}
		}
		m_ErrCode = IsConnected();
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			if( m_ErrCode >= 0 )
				m_ErrCode = -1;

			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
		}
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		TPI_MoveFirst( hSet );
		AddMapUnivCitedNum(hSet,m_AuthorCodeCitedNumMiddle,nRecordCount,pTable,m_CurUnivCode);
		m_CurUnivCode.clear();
		TPI_CloseRecordSet( hSet );
	}


	sprintf( szSql, "SELECT 机构所有代码 as allcode,机构代码 as code,机构名称 as name from %s",pSourceTable );
	m_ErrCode = IsConnected();
	hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
	TPI_MoveFirst( hSet );
	AddMapUnivCitedNumMulti(hSet,m_AuthorCodeCitedNum,nRecordCount,pTable);
	TPI_CloseRecordSet( hSet );
	return m_ErrCode;
}

int	CKBConn::AddUnivItem(char * pZtCode, char * pTable, string strWhere, char *pField, char *pCodeName, char *pDict)
{
	map<hfsstring,int>::iterator itt;
	string strUnivCode = "";
	char szSql[MAX_SQL]={0};
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	string strZtCode = pZtCode;
	string strCurUnivCode = "";
	m_UnivZwl.clear();
	m_UnivXzl.clear();
	m_UnivHx.clear();
	m_UnivSci.clear();
	m_UnivEi.clear();
	m_UnivFun.clear();
	m_CurUnivCode.clear();
	vector<string> items;
	items.clear();
	CUtility::Str()->CStrToStrings(strWhere.c_str(),items,'+');
	int iCount = items.size();
	for (int i = 0;i < iCount;++i)
	{
		strCurUnivCode += items[i];
		strCurUnivCode += "+";
		strUnivCode = items[i];
		m_CurUnivCode.insert(make_pair(strUnivCode,0));
		if ((i%20 == 0)||(i == iCount-1))
		{
			strCurUnivCode = strCurUnivCode.substr(0,strCurUnivCode.length()-1);
			strWhere = strCurUnivCode;
			//载文量
			sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
			m_ErrCode = AddUnivItem(szSql,m_UnivZwl,m_CurUnivCode);

			//核心期刊载文量
			sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y group by (%s,%s)", pField, "cjfdtotal", pField,strWhere.c_str(), pZtCode, pField, pDict );
			m_ErrCode = AddUnivItem(szSql,m_UnivHx,m_CurUnivCode);

			//SCI载文量
			sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
			m_ErrCode = AddUnivItem(szSql,m_UnivSci,m_CurUnivCode);

			//EI载文量
			sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and  EI收录刊=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
			m_ErrCode = AddUnivItem(szSql,m_UnivEi,m_CurUnivCode);

			//基金论文数
			sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
			m_ErrCode = AddUnivItem(szSql,m_UnivFun,m_CurUnivCode);
			strCurUnivCode = "";
			m_CurUnivCode.clear();
		}
	}
	return m_ErrCode;
}


int	CKBConn::AddUnivItem(char * szSql,map<string,int> &map,std::map<string,int>&univ)
{
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	m_ErrCode = IsConnected();
	hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在AddUnivItem中  sql %s]",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*20);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	hfsstring strCode = "";
	hfsstring strNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	std::map<hfsstring,int>::iterator it;
	for (int i = 1;i <= nRecordCount;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"code",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCode = t;
		}
		//counts
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"counts",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strNum = t;
			nCitedNum = (int)_atoi64(t);
		}
		it = univ.find(strCode);
		if (it != univ.end())
		{
			it = map.find(strCode);
			if (it == map.end())
			{
				map.insert(make_pair(strCode,nCitedNum));
			}
		}
		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );
	return m_ErrCode;
}

int	CKBConn::AddAuthItem(char * pZtCode, char * pTable, string strWhere, char *pField, char *pCodeName, char *pDict)
{
	map<hfsstring,int>::iterator itt;
	string strAuthorCode = "";
	char szSql[MAX_SQL]={0};
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	string strZtCode = pZtCode;
	m_UnivZwl.clear();
	m_UnivXzl.clear();
	m_UnivHx.clear();
	m_UnivSci.clear();
	m_UnivEi.clear();
	m_UnivFun.clear();
	//载文量
	sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
	m_ErrCode = AddAuthItem(szSql,m_UnivZwl);

	//下载量  因为超时的原因，这样反而更耗时间?
	//分着计算的总的时间也很多，还是总起来快？
	//sprintf( szSql, "select %s as code,sum(下载频次) as counts from %s where %s=%s and 专题子栏目代码=%s? not 引证标识=0 group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
	//AddUnivItem(szSql,m_UnivXzl);

	//核心期刊载文量
	sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and 核心期刊=Y group by (%s,%s)", pField, "cjfdtotal", pField,strWhere.c_str(), pZtCode, pField, pDict );
	m_ErrCode = AddAuthItem(szSql,m_UnivHx);

	//SCI载文量
	sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and SCI收录刊=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
	m_ErrCode = AddAuthItem(szSql,m_UnivSci);

	//EI载文量
	sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and  EI收录刊=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
	m_ErrCode = AddAuthItem(szSql,m_UnivEi);

	//基金论文数
	sprintf( szSql, "select %s as code,count(*) as counts from %s where %s=%s and 专题子栏目代码=%s? and 是否基金文献=Y group by (%s,%s)", pField, "cjfdtotal", pField, strWhere.c_str(), pZtCode, pField, pDict );
	m_ErrCode = AddAuthItem(szSql,m_UnivFun);

	return m_ErrCode;
}

int	CKBConn::AddAuthItem(char * szSql,map<string,int> &map)
{
	TPI_HRECORDSET hSet;
	int nRecordCount = 0;
	m_ErrCode = IsConnected();
	hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在AddAuthItem中  sql %s]",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*20);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	hfsstring strCode = "";
	hfsstring strNum = "";
	string strFist = "";
	int nCitedNum = 0;
	int nCount = 0;
	std::map<hfsstring,int>::iterator it;
	for (int i = 1;i <= nRecordCount;++i){
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"code",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strCode = t;
		}
		//counts
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"counts",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strNum = t;
			nCitedNum = (int)_atoi64(t);
		}
		map.insert(make_pair(strCode,nCitedNum));
		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );
	return m_ErrCode;
}

int		CKBConn::UpdateTableHosp(string strNumber,string strValue,string strTable)
{
	char sql[1024] = {0};
	vector<string> items;
	items.clear();
	CUtility::Str()->CStrToStrings(strValue.c_str(),items,',');
	if (items.size() < 6)
	{
		return 0;
	}
	hfschar cTemp[64]={0};
	hfsstring strSql = "UPDATE ";
	strSql += strTable;
	strSql += " SET 机构编码_变化前=";
	if (items[0] =="NULL")
	{
		strSql += "'";
		strSql += items[0];
		strSql += "'";
	}
	else
	{
		strSql += items[0];
	}
	strSql += ",变化前名称开始年 =";
	if (items[1] =="NULL")
	{
		strSql += "'";
		strSql += items[1];
		strSql += "'";
	}
	else
	{
		strSql += items[1];
	}
	strSql += ",变化前名称开始月 =";
	if (items[2] =="NULL")
	{
		strSql += "'";
		strSql += items[2];
		strSql += "'";
	}
	else
	{
		strSql += items[2];
	}
	strSql += ",变化前名称结束年 =";
	if (items[3] =="NULL")
	{
		strSql += "'";
		strSql += items[3];
		strSql += "'";
	}
	else
	{
		strSql += items[3];
	}
	strSql += ",变化前名称结束月 =";
	if (items[4] =="NULL")
	{
		strSql += "'";
		strSql += items[4];
		strSql += "'";
	}
	else
	{
		strSql += items[4];
	}
	strSql += ",机构编码_变化后 =";
	if (items[5] =="NULL")
	{
		strSql += "'";
		strSql += items[5];
		strSql += "'";
	}
	else
	{
		strSql += items[5];
	}
	strSql += " where 序号 = ";
	strSql += strNumber;
	strcpy(sql,strSql.c_str());

	hfsint e = ExecuteCmd(sql);

	return e;
}

int		CKBConn::StatDoc(DOWN_STATE * taskData,char *pRetFile)
{
	char pTable[255] = {0};
	int nCount = 0;
	strcpy(pTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );
	vector<ST_ZJCLS *>::iterator it;
	string strUniqField = "";
	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, pTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	char szMsg[512];
	FILE *pFile = OpenRecFile( szFileName );
	if( !pFile )///may failed
	{
		sprintf( szMsg, "创建文件%s失败", szFileName );
		CUtility::Logger()->d("error info.[%s]",szMsg);

	    return -1;
	}
	char szSql[1024] = {0};
	int nErrCode = 0;
	long nLen = 0;
	char szBuff[64] = {0};
	int nRet = 0;
	ST_DOCU item;
	TPI_HRECORDSET hSet;
	for( it =  m_ZJCls.begin(); it !=  m_ZJCls.end(); ++ it )
	{
		memset(szSql,0,sizeof(szSql));
		sprintf( szSql, "select * from %s where 专题子栏目代码 = %s? order by (被引频次, 'integer') DESC", "cjfdref",(*it)->szCode );
		//CUtility::Logger()->d("exec sql info.[%s]",szSql);

		nErrCode = 0;
		m_ErrCode = IsConnected();
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || nErrCode < 0 )
		{
			char szMsg[1024];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, nErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
		
			if( nErrCode >= 0 )
				nErrCode = -1;

			return nErrCode;
		}
		nRet = TPI_GetRecordSetCount( hSet );
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		TPI_MoveFirst( hSet );

		for( int i = 0; i < GETRECORDNUM*2; i ++ )
		{
	
			memset( &item, 0, sizeof(item) );		
       
			//题名
			nLen = (int)sizeof(item.szName);
			nLen = TPI_GetFieldValueByName( hSet, "题名", item.szName, nLen );
			if( item.szName[0] == '\0' )
			{
				//TPI_MoveNext( hSet );
				//continue;
			}

			//作者
			nLen = (int)sizeof(item.szAuthor);
			nLen = TPI_GetFieldValueByName( hSet, "作者", item.szAuthor, nLen );
			if( item.szAuthor[0] == '\0' )
			{
				TPI_MoveNext( hSet );
				continue;
			}

			//单位
			nLen = (int)sizeof(item.szDept);
			nLen = TPI_GetFieldValueByName( hSet, "单位", item.szDept, nLen );
			if( item.szDept[0] == '\0' )
			{
				//TPI_MoveNext( hSet );
				//continue;
			}

			//文件名
			nLen = (int)sizeof(item.szFileName);
			nLen = TPI_GetFieldValueByName( hSet, "文件名", item.szFileName, nLen );
			if( item.szFileName[0] == '\0' )
			{
				TPI_MoveNext( hSet );
				continue;
			}
		    
			//表名
			nLen = (int)sizeof(item.szTable);
			nLen = TPI_GetFieldValueByName( hSet, "表名", item.szTable, nLen );
			if( item.szTable[0] == '\0' )
			{
				TPI_MoveNext( hSet );
				continue;
			}
		    
			//期刊范围
			nLen = (int)sizeof(item.szJRange);
			TPI_GetFieldValueByName( hSet, "期刊范围", item.szJRange, nLen );
		
			//来源
			nLen = (int)sizeof(item.szSource);
			TPI_GetFieldValueByName( hSet, "来源", item.szSource, nLen );
		
			//年
			nLen = (int)sizeof(item.szYear);
			TPI_GetFieldValueByName( hSet, "年", item.szYear, nLen );
		
			//期
			nLen = (int)sizeof(item.szPeriod);
			TPI_GetFieldValueByName( hSet, "期", item.szPeriod, nLen );
		
			//自社科
			nLen = (int)sizeof(item.szZSK);
			TPI_GetFieldValueByName( hSet, "自社科", item.szZSK, nLen );		
		
			//被引频次
			nLen = (int)sizeof(szBuff);
			memset( szBuff, 0, nLen );
			TPI_GetFieldValueByName( hSet, "被引频次", szBuff, nLen );	
			item.nRefTimes = atoi(szBuff);
		
			//他引频次
			nLen = (int)sizeof(szBuff);
			memset( szBuff, 0, nLen );
			TPI_GetFieldValueByName( hSet, "他引频次", szBuff, nLen );	
			item.nTRefTimes = atoi(szBuff);

			//他引率
			int nFm = (int)(item.nTRefTimes*1000/item.nRefTimes);
			item.nTRefRate = ((float)(nFm+5))/1000;
			int nRate = item.nTRefRate*100;
			item.nTRefRate = (float)nRate/100;

			//下载频次
			nLen = (int)sizeof(szBuff);
			memset( szBuff, 0, nLen );
			TPI_GetFieldValueByName( hSet, "下载频次", szBuff, nLen );
			item.nDownTimes = atoi(szBuff);

			//唯一键值
			strUniqField = (*it)->szCode;
			strUniqField += item.szFileName;
			strcpy(item.szUniqField,strUniqField.c_str());

			strcpy(item.szDate,szDate);

			strcpy(item.szZtCode,(*it)->szCode);
			strcpy(item.szZtName,(*it)->szName);
        
			WriteRecFileDOCU( pFile, &item );
			++nCount;
			if (nCount >= GETRECORDNUM)
			{
				break;
			}

			TPI_MoveNext( hSet );
		}
		TPI_CloseRecordSet( hSet );
		nCount = 0;
		fflush(pFile);
	}

	CloseRecFile( pFile );
	if( m_ErrCode >= 0 && nRet > 0 )
	{
		nErrCode = nCount + 1;
		strcpy( pRetFile, szFileName );
	}

	return nErrCode;

}

int		CKBConn::StatDocUpdate(DOWN_STATE * taskData,char *pRetFile)
{
	char pTable[255] = {0};
	int nCount = 0;
	strcpy(pTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );
	vector<ST_ZJCLS *>::iterator it;
	string strUniqField = "";
	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, pTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	char szMsg[512];
	FILE *pFile = OpenRecFile( szFileName );
	if( !pFile )///may failed
	{
		sprintf( szMsg, "创建文件%s失败", szFileName );
		CUtility::Logger()->d("error info.[%s]",szMsg);

	    return -1;
	}
	char szSql[1024] = {0};
	int nErrCode = 0;
	long nLen = 0;
	char szBuff[64] = {0};
	int nRet = 0;
	ST_DOCU item;
	TPI_HRECORDSET hSet;
	int iCount = 0;
	vector<string> items;
	string strCodeUniqCode = "";
	for( it =  m_ZJCls.begin(); it !=  m_ZJCls.end(); ++ it )
	{
		memset(szSql,0,sizeof(szSql));
		sprintf( szSql, "select 文件名 as code,唯一键值 as uniqCode from %s where 专题代码 = %s? ", "DOCU_REF",(*it)->szCode );
		m_vCodeUniqCode.clear();
		LoadFieldInfo(szSql,m_vCodeUniqCode);
		iCount = m_vCodeUniqCode.size();

		for (int i = 0;i < iCount;++i)
		{
			strCodeUniqCode = m_vCodeUniqCode[i];
			items.clear();
			CUtility::Str()->CStrToStrings(strCodeUniqCode.c_str(),items,',');
			int iCount = items.size();
			if (iCount > 1)
			{
			}
			else
			{
				continue;
			}
			memset(szSql,0,sizeof(szSql));
			sprintf( szSql, "select * from %s where 专题子栏目代码 = %s? and 文件名 = %s", "cjfdref",(*it)->szCode, items[0].c_str());
			nErrCode = 0;
			m_ErrCode = IsConnected();
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
			{
				CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
				Sleep( 1000 );
				hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			}
			if( !hSet || nErrCode < 0 )
			{
				char szMsg[1024];
				sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, nErrCode );
				CUtility::Logger()->d("error info.[%s]",szMsg);
		
				if( nErrCode >= 0 )
					nErrCode = -1;

				return nErrCode;
			}
			nRet = TPI_GetRecordSetCount( hSet );
			m_ErrCode = TPI_SetRecordCount( hSet, 1);
			TPI_MoveFirst( hSet );

			for( int i = 0; i < 1; i ++ )
			{
	
				memset( &item, 0, sizeof(item) );		
       

				//文件名
				nLen = (int)sizeof(item.szFileName);
				nLen = TPI_GetFieldValueByName( hSet, "文件名", item.szFileName, nLen );
				if( item.szFileName[0] == '\0' )
				{
					//TPI_MoveNext( hSet );
					//continue;
				}
		    
				//期刊范围
				nLen = (int)sizeof(item.szJRange);
				TPI_GetFieldValueByName( hSet, "期刊范围", item.szJRange, nLen );
		
				//来源
				nLen = (int)sizeof(item.szSource);
				TPI_GetFieldValueByName( hSet, "来源", item.szSource, nLen );
		
				//年
				nLen = (int)sizeof(item.szYear);
				TPI_GetFieldValueByName( hSet, "年", item.szYear, nLen );
		
				//期
				nLen = (int)sizeof(item.szPeriod);
				TPI_GetFieldValueByName( hSet, "期", item.szPeriod, nLen );
		
				//自社科
				nLen = (int)sizeof(item.szZSK);
				TPI_GetFieldValueByName( hSet, "自社科", item.szZSK, nLen );		
		
				//被引频次
				nLen = (int)sizeof(szBuff);
				memset( szBuff, 0, nLen );
				TPI_GetFieldValueByName( hSet, "被引频次", szBuff, nLen );	
				item.nRefTimes = atoi(szBuff);
		
				//他引频次
				nLen = (int)sizeof(szBuff);
				memset( szBuff, 0, nLen );
				TPI_GetFieldValueByName( hSet, "他引频次", szBuff, nLen );	
				item.nTRefTimes = atoi(szBuff);

				//他引率
				int nFm = (int)(item.nTRefTimes*1000/item.nRefTimes);
				item.nTRefRate = ((float)(nFm+5))/1000;
				int nRate = item.nTRefRate*100;
				item.nTRefRate = (float)nRate/100;

				//下载频次
				nLen = (int)sizeof(szBuff);
				memset( szBuff, 0, nLen );
				TPI_GetFieldValueByName( hSet, "下载频次", szBuff, nLen );
				item.nDownTimes = atoi(szBuff);

				//唯一键值
				strcpy(item.szUniqField,items[1].c_str());

				strcpy(item.szDate,szDate);

				strcpy(item.szZtCode,(*it)->szCode);
				strcpy(item.szZtName,(*it)->szName);
        
				WriteRecFileDOCUUpdate( pFile, &item );
				}
		}
		TPI_CloseRecordSet( hSet );
		++nCount;
		fflush(pFile);
		Sleep(1000);
	}

	CloseRecFile( pFile );
	if( m_ErrCode >= 0 )
	{
		nErrCode = nCount;
		strcpy( pRetFile, szFileName );
	}

	return nErrCode;

}

int CKBConn::WriteRecFileDOCU(FILE *pFile, ST_DOCU *pItem)
{
	if( !pFile || !pItem )
		return -1;

	WriteRecFile( pFile, "唯一键值", pItem->szUniqField, false );

	WriteRecFile( pFile, "文件名", pItem->szFileName, true );

	WriteRecFile( pFile, "表名", pItem->szTable, true );
    
	WriteRecFile( pFile, "题名", pItem->szName, true );
	
	WriteRecFile( pFile, "作者", pItem->szAuthor, true );

	WriteRecFile( pFile, "单位", pItem->szDept, true );

	WriteRecFile( pFile, "期刊范围", pItem->szJRange, true );
	
	WriteRecFile( pFile, "来源", pItem->szSource, true );

	WriteRecFile( pFile, "年", pItem->szYear, true );
	
	WriteRecFile( pFile, "期", pItem->szPeriod,  true );

	WriteRecFile( pFile, "自社科", pItem->szZSK, true );
	
	WriteRecFile( pFile, "下载频次", pItem->nDownTimes, true );
   
	WriteRecFile( pFile, "被引频次", pItem->nRefTimes, true );

	WriteRecFile( pFile, "专题代码", pItem->szZtCode, true );

	WriteRecFile( pFile, "专题名称", pItem->szZtName, true );

	WriteRecFile( pFile, "他引频次", pItem->nTRefTimes, true );

	WriteRecFileFloat( pFile, "他引率", pItem->nTRefRate, true );

	WriteRecFile( pFile, "更新日期", pItem->szDate, true );

	return 0;
}

int CKBConn::WriteRecFileDOCUUpdate(FILE *pFile, ST_DOCU *pItem)
{
	if( !pFile || !pItem )
		return -1;

	WriteRecFile( pFile, "唯一键值", pItem->szUniqField, false );

	WriteRecFile( pFile, "文件名", pItem->szFileName, true );

	//WriteRecFile( pFile, "表名", pItem->szTable, true );
    
	//WriteRecFile( pFile, "题名", pItem->szName, true );
	
	//WriteRecFile( pFile, "作者", pItem->szAuthor, true );

	//WriteRecFile( pFile, "单位", pItem->szDept, true );

	//WriteRecFile( pFile, "期刊范围", pItem->szJRange, true );
	
	//WriteRecFile( pFile, "来源", pItem->szSource, true );

	//WriteRecFile( pFile, "年", pItem->szYear, true );
	
	WriteRecFile( pFile, "期", pItem->szPeriod,  true );

	WriteRecFile( pFile, "自社科", pItem->szZSK, true );
	
	WriteRecFile( pFile, "下载频次", pItem->nDownTimes, true );
   
	WriteRecFile( pFile, "被引频次", pItem->nRefTimes, true );

	WriteRecFile( pFile, "专题代码", pItem->szZtCode, true );

	WriteRecFile( pFile, "专题名称", pItem->szZtName, true );

	WriteRecFile( pFile, "他引频次", pItem->nTRefTimes, true );

	WriteRecFileFloat( pFile, "他引率", pItem->nTRefRate, true );

	WriteRecFile( pFile, "更新日期", pItem->szDate, true );

	return 0;
}

hfsbool		CKBConn::CheckDataUniv(string strSourceTable,string strTargetTable)
{
	int m_ErrCode = IsConnected();
	int iRecordCount = 0;
	char szSql[1024] = {0};
	sprintf( szSql, "select 机构编码,当前标准一级机构 as 机构名称 ,count(*) as Counts from %s group by 机构编码 order by count(*) desc", strSourceTable.c_str() );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	iRecordCount  = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, 500);
	char jgdm[32] = {0};
	char jgName[255] = {0};
	char jgdms[8] = {0};
	string strJgCode = "";
	string strJgName = "";
	string strAllJgCode = "";
	string strTable = strTargetTable;
	string strJgdms = "";
	long nLen;
	int iCount = 0;
	TPI_MoveFirst( hSet );
	
	for( int i = 0; i < iRecordCount; i ++ )
	{
		memset( jgdm, 0, sizeof(jgdm) );
		memset( jgName, 0, sizeof(jgName) );
		nLen = (int)sizeof(jgdm);
		TPI_GetFieldValue( hSet, 0, jgdm, nLen );
		strJgCode = jgdm;
		nLen = (int)sizeof(jgName);
		TPI_GetFieldValue( hSet, 1, jgName, nLen );
		strJgName = jgName;
		nLen = (int)sizeof(jgdms);
		TPI_GetFieldValue( hSet, 2, jgdms, nLen );
		strJgdms = jgdms;
		InsertQueryUnivAll(strSourceTable,strTargetTable,strJgCode,jgName,strJgdms);

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsint	CKBConn::InsertQueryUnivAll(string strTableSource,string strTableTarget,string strJgCode,string strJgName,string strNum)
{
	m_ErrCode = IsConnected();
	map<string,int> m_jg;
	int iRecordCount = 0;
	char szSql[1024] = {0};
	sprintf( szSql, "select 机构编码,机构编码_变化后 ,机构编码_变化前 from %s where 机构编码=%s", strTableSource.c_str(),strJgCode.c_str() );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	iRecordCount  = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, 100);
	char jgdma[32] = {0};
	char jgdmb[32] = {0};
	int nJgdmCount = 0;
	char jgdms[8] = {0};
	string strJgCodea = "";
	string strAllJgCode = "";
	string strJgdms = "";
	long nLen;
	int iCount = 0;
	TPI_MoveFirst( hSet );
	map<string,int>::iterator it;
	
	for( int i = 0; i < iRecordCount; i ++ )
	{
		memset( jgdma, 0, sizeof(jgdma) );
		memset( jgdmb, 0, sizeof(jgdmb) );
		nLen = (int)sizeof(jgdma);
		TPI_GetFieldValue( hSet, 1, jgdma, nLen );
		strJgCodea = jgdma;
		if (strJgCodea != "NULL")
		{
			it = m_jg.find(strJgCodea);
			if(it == m_jg.end()){
				m_jg.insert(make_pair(strJgCodea,1));
			}
		}
		nLen = (int)sizeof(jgdmb);
		TPI_GetFieldValue( hSet, 2, jgdmb, nLen );
		strJgCodea = jgdmb;
		if (strJgCodea != "NULL")
		{
			it = m_jg.find(strJgCodea);
			if(it == m_jg.end()){
				m_jg.insert(make_pair(strJgCodea,1));
			}
		}
		TPI_MoveNext( hSet );
	}
	nJgdmCount = m_jg.size();
	for (it=m_jg.begin();it!=m_jg.end();++it)
	{
		strJgCodea = (*it).first;
		strAllJgCode += strJgCodea;
		strAllJgCode += "+";
	}
	strAllJgCode = strAllJgCode.substr(0,strAllJgCode.length()-1);
	InsertQueryUniv(szSql,strTableTarget,strJgCode,strAllJgCode,strJgName,nJgdmCount);
	hfsint e = ExecuteCmd(szSql);

	TPI_CloseRecordSet(hSet);

	return e;
}

hfsint	CKBConn::InsertQueryUniv(phfschar sql,string strTable,string strJgCode,string strAllJgCode,string strJgName,int iNum)
{
	char t[10] = {0};
	string strQuery = "INSERT INTO ";
	strQuery += strTable;
	strQuery +="(机构代码,机构所有代码,机构名称,机构代码数)";
	strQuery += " VALUES(";
	strQuery += strJgCode;
	strQuery += ",";
	strQuery += "'";
	strQuery += strAllJgCode;
	strQuery += "'";
	strQuery += ",";
	if (strJgName == "")
	{
		strQuery += "'";
		strQuery += "'";
	}
	else
	{
		strQuery += "'";
		strQuery += strJgName;
		strQuery += "'";
	}
	strQuery += ",";
	memset(t,0,sizeof(t));
	sprintf(t,"%d",iNum);
	strQuery += t;
	strQuery += ")";
	strcpy(sql,strQuery.c_str());
	return true;
}

hfsbool		CKBConn::CheckDataHospUniv(string strSourceTable,string strTargetTable)
{
	int m_ErrCode = IsConnected();
	int iRecordCount = 0;
	char szSql[1024] = {0};
	sprintf( szSql, "select 机构代码 as code,机构所有代码 as allcode from %s ", strSourceTable.c_str() );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	iRecordCount  = TPI_GetRecordSetCount( hSet );
	m_ErrCode = TPI_SetRecordCount( hSet, 500);
	char jgdm[32] = {0};
	char jgallcode[2048] = {0};
	string strJgCode = "";
	string strAllJgCode = "";
	string strTable = strTargetTable;
	long nLen;
	int iCount = 0;
	TPI_MoveFirst( hSet );
	
	for( int i = 0; i < iRecordCount; i ++ )
	{
		memset( jgdm, 0, sizeof(jgdm) );
		memset( jgallcode, 0, sizeof(jgallcode) );
		nLen = (int)sizeof(jgdm);
		TPI_GetFieldValue( hSet, 0, jgdm, nLen );
		strJgCode = jgdm;
		nLen = (int)sizeof(jgallcode);
		TPI_GetFieldValue( hSet, 1, jgallcode, nLen );
		strAllJgCode = jgallcode;
		int pos = strAllJgCode.find(strJgCode);
		if (pos == -1)
		{
			InsertQueryUniv(szSql,strTargetTable,strJgCode,strAllJgCode);
		}

		hfsint e = ExecuteCmd(szSql);

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsint	CKBConn::InsertQueryUniv(phfschar sql,string strTable,string strJgCode,string strAllJgCode)
{
	char t[10] = {0};
	string strQuery = "INSERT INTO ";
	strQuery += strTable;
	strQuery +="(机构代码,机构所有代码)";
	strQuery += " VALUES(";
	strQuery += strJgCode;
	strQuery += ",";
	strQuery += "'";
	strQuery += strAllJgCode;
	strQuery += ")";
	strcpy(sql,strQuery.c_str());
	return true;
}

hfsint	CKBConn::GetItemsNumber(char * szCode,map<hfsstring,int>&map,long long &nResult)
{
	std::map<hfsstring,int>::iterator it;
	vector<string> items;
	int nItemsCount = 0;
	items.clear();
	CUtility::Str()->CStrToStrings(szCode,items,'+');
	nItemsCount = items.size();
	if (nItemsCount < 2)
	{
		it = map.find(szCode);
		if (it != map.end())
		{
			nResult = (*it).second;
		}
		else
		{
			nResult = 0;
		}
	}
	else
	{
		nResult = 0;
		for (int i=0;i < nItemsCount;++i)
		{
			it = map.find(items[i]);
			if (it != map.end())
			{
				nResult += (*it).second;
			}
			else
			{
				nResult += 0;
			}
		}
	}
	return 1;
}

void CKBConn::UniqVectorItem(vector<pair<string,int>>& sVector,vector<pair<string,int>>& tVector)
{
	map<string,int> tempMap;
	vector<string> items;
	tempMap.clear();
	string strCode = "";
	int iItems = 0;
	int iCount = sVector.size();
	map<string,int>::iterator it;
	int nRecordCount = 0;

	for(int i = 0; i < iCount; ++i)
	{
		string sTemp2="";
		strCode = sVector[i].first;
		sTemp2 = strCode;
		items.clear();
		CUtility::Str()->CStrToStrings(strCode.c_str(),items,';');
		iItems = items.size();
		if (iItems > 0)
		{
			strCode = items[0];
			items.clear();
			CUtility::Str()->CStrToStrings(strCode.c_str(),items,'+');
		}
		iItems = items.size();
		if (iItems == 1)
		{
			continue;
		}
		else if(iItems > 1)
		{
			string sTemp;
			for (int j = 0;j < iItems;++j)
			{
				sTemp = items[j];
				tempMap.insert(make_pair(items[j],1));
			}
		}
		++nRecordCount;
		if (GETRECORDNUM*2 <= nRecordCount)
		{
			break;
		}
	}
	nRecordCount = 0;
	for(int i = 0; i < iCount; ++i)
	{
		strCode = sVector[i].first;
		items.clear();
		CUtility::Str()->CStrToStrings(strCode.c_str(),items,';');
		iItems = items.size();
		if (iItems > 0)
		{
			strCode = items[0];
			items.clear();
			CUtility::Str()->CStrToStrings(strCode.c_str(),items,'+');
		}
		iItems = items.size();
		if (iItems == 1)
		{
			it = tempMap.find(items[0]);
			if(it != tempMap.end())
			{
				continue;
			}
		}
		tVector.push_back(sVector[i]);
		++nRecordCount;
		if (GETRECORDNUM*1.5 <= nRecordCount)
		{
			break;
		}
	}
}

int CKBConn::RunDownLoad(DOWN_STATE * taskData)
{
	map<string,string>pykmMap;
	map<string,string>::iterator itMap;
	LoadCJFDBaseinfo( pykmMap );

	vector<string>strDownFileList;

	CFolder f;
	vector<string>strFileList;
	vector<string>::iterator it2;
	f.GetFilesList( taskData->szLocPath, "*.zip", strFileList );

	SetCurrentDirectory( taskData->szLocDirectory );
	string strFilePathCurrent = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
		HZIP hz = OpenZip( (char *)it2->c_str(),0,ZIP_FILENAME );
		ZIPENTRY ze; 
		GetZipItem( hz, -1, &ze ); 
		int numitems = ze.index;
		for( int i = 0; i < numitems; i ++ )
		{ 
			GetZipItem( hz, i, &ze );
			SetCurrentDirectory( taskData->szLocDirectory );
			strFilePathCurrent = taskData->szLocDirectory;
			strFilePathCurrent += "\\";
			strFilePathCurrent += ze.name;
			_chmod( strFilePathCurrent.c_str(), _S_IWRITE );
			::DeleteFile( strFilePathCurrent.c_str() );

			if( UnzipItem( hz, i, ze.name, 0, ZIP_FILENAME ) == ZR_OK )
			{
				strDownFileList.push_back( ze.name );
				//_chmod( ze.name, _S_IWRITE );
				//DeleteFile( ze.name );
			}
			else
			{
				break;
			}
		}
		CloseZip( hz );

	}

	HS_MapStrToInt hash;
	hash.AllocMemoryPool();
	hash.InitHashTable(4096);
	int nptrVal;

	char szKey[18];

	char szLine[4096] = {0};
	vector<string> items;
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char szFileName[64];
	char szPubYear[5], szDownYear[5], szMon[3];
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator it;
	string strFilePath="";
	for( it = strDownFileList.begin(); it != strDownFileList.end(); ++ it )
	{
		try
		{
			strncpy( szDownYear, it->c_str() + 1, 4 );
			szDownYear[4] = '\0';
			strncpy( szMon, it->c_str() + 5, 2 );
			szMon[2] = '\0';
			if( szMon[0] == '0' )
			{
				szMon[0] = szMon[1];
				szMon[1] = '\0';
			}

			SetCurrentDirectory( taskData->szLocDirectory );
			fp=fopen(it->c_str(),"r");  
			if (!fp)
			{
				continue;
			}
			int i=0;  
			string sPreLine = "";
			string sCurLine = "";
			string sFileName = "";
			string::size_type   pos(0);     
			char szFileName[18];
			char szCode[5];
			cChar=fgetc(fp);  
			while(!feof(fp))  
			{  
				//try
				//{
					if (cChar=='\r')
					{
						cChar=fgetc(fp);  
						continue;
					}
					if (cChar!='\n')
					{
						szLine[i]=cChar;  
					}
					else
					{
						szLine[i]='\0'; 
						sCurLine = szLine;
						if( (pos=sCurLine.find("\t"))!=string::npos )
						{
							if (pos >= 4)
							{
								memset(szCode,0,sizeof(szCode));
								memcpy(szCode,(sCurLine.substr(pos - 4,4)).c_str(),4);
								_strupr( szCode );
							}
						}

						pBegin = strchr( szLine, '\t' );
						if( !pBegin )
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}

						pBegin ++;

						pEnd = strrchr( szLine, '\t' );
						if( !pEnd )
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}
						*pEnd = '\0';

						strncpy( szFileName, pBegin, sizeof(szFileName) - 1 );
						_strupr( szFileName );
						sFileName = szFileName;
						if (sFileName.length() < 7)
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}
						if( IsCJFD(  NULL, szFileName, szCode ) && GetPubYear( szFileName, szPubYear ) )
						{
							memset( szKey, 0, sizeof(szKey) );
							strncpy( szKey, szFileName, 4 );
							strcat( szKey, szPubYear );
							strcat( szKey, "-" );
							strcat( szKey, szDownYear );
							strcat( szKey, ":" );
							strcat( szKey, szMon );
							if( hash.Lookup( szKey, nptrVal ) )
							{
								hash.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hash.SetAt( szKey, 1 );
							}		
						}
						bInsert = true;
					}
					if (!bInsert)
					{
						++i;  
					}
					else
					{
						i = 0;
						memset( szLine, 0, sizeof(szLine) );
						bInsert = false;
					}
					cChar=fgetc(fp);  
				//}
				//catch(exception ex)
				//{
				//	CUtility::Logger()->d("exception .");

				//}
			}  
			if (fp)	fclose(fp);
			Sleep(500);
			strFilePath = taskData->szLocDirectory;
			strFilePath += "\\";
			strFilePath += *it;
			_chmod( strFilePath.c_str(), _S_IWRITE );
			::DeleteFile( strFilePath.c_str() );
		}
		catch(exception ex)
		{
			CUtility::Logger()->d("exception happen.");
		}
	}
	int i=0;  

	int nRet = 0;
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20];
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFile[MAX_PATH];
	GetModuleFileName( NULL, szFile, sizeof(szFile) );
	strcpy( strrchr(szFile, '\\') + 1, szDate );
	strcat( szFile, ".txt" );
	::DeleteFile( szFile );
	FILE *hFile = OpenRecFile( szFile );

	
	char szBuff[64];
	char szPYKM[5];
	char szID[16];
	HITEM_POSITION pos;
	char *key;
	for( pos = hash.GetStartPosition(); pos != NULL; )
	{
		hash.GetNextAssoc( pos, key, nptrVal );
		
		memset( szPYKM, 0, 5 );
		strncpy( szPYKM, key, 4 );

		memset( szPubYear, 0, 5 );
		strncpy( szPubYear, key + 4, 4 );

		memset( szDownYear, 0, 5 );
		strncpy( szDownYear, key + 9, 4 );

		memset( szMon, 0, 3 );
		strcpy( szMon, strchr(key, ':') + 1 );

		WriteRecFile( hFile, "拼音刊名", szPYKM, false );
		itMap = pykmMap.find( szPYKM );
		if( itMap != pykmMap.end() )
			WriteRecFile( hFile, "刊名", (char *)itMap->second.c_str(), true );

		WriteRecFile( hFile, "出版年度", szPubYear, true );
		WriteRecFile( hFile, "下载年度", szDownYear, true );
		WriteRecFile( hFile, "下载月度", szMon, true );

		WriteRecFile( hFile, "下载频次", nptrVal, true );

		if( szMon[1] == '\0' )
		{
			szMon[1] = szMon[0];
			szMon[0] = '0';
		}
		sprintf( szBuff, "%s%s", szDownYear, szMon );
		WriteRecFile( hFile, "下载日期", szBuff, true );

		sprintf( szID, "%s%s_%s%s", szPYKM, szPubYear, szDownYear, szMon );
		WriteRecFile( hFile, "ID", szID, true );

		//WriteRecFile( hFile, "更新日期", szDate, true );
	}
	if (hFile) fclose( hFile );
	hash.RemoveAll();
	hash.FreeMemoryPool();
	nRet = AppendTable( taskData->tablename, szFile );
	//nRet = SortTable( taskData->nStatType);

	return nRet;
}

int CKBConn::RunDownLoadTrue(DOWN_STATE * taskData)
{
	////begin
	//string sPath="E:\\yxwhole";
	//string sFileName = "E:\\yxwhole\\HKXB201502.pdf";
	//string sFileNameXml = "E:\\yxwhole\\HKXB201501.xml";
	//string sFileName10 = "E:\\yxwhole\\HKXB20150110.pdf";
	//string sFileName22 = "E:\\yxwhole\\HKXB20150122.pdf";
	//string sFileName23 = "E:\\yxwhole\\HKXB20150123.pdf";
	//string sFileName26 = "E:\\yxwhole\\HKXB20150126.pdf";
 //   char sPdfName[256];
 //   char sXmlName[256];
 //   char sPdfName10[256];
 //   char sPdfName22[256];
 //   char sPdfName23[256];
 //   char sPdfName26[256];
 //   for (int i = 4; i < 50;i++ )
 //   {
	//	memset(sPdfName,0,sizeof(sPdfName));
	//	memset(sXmlName,0,sizeof(sXmlName));
	//	memset(sPdfName10,0,sizeof(sPdfName10));
	//	memset(sPdfName22,0,sizeof(sPdfName22));
	//	memset(sPdfName23,0,sizeof(sPdfName23));
	//	memset(sPdfName26,0,sizeof(sPdfName26));
 //       if (i < 10)
 //       {
	//		sprintf( sPdfName,"%s\\HKXB20150%d.pdf", sPath.c_str(), i );
	//		sprintf( sXmlName,"%s\\HKXB20150%d.xml", sPath.c_str(), i );
 //       }
 //       else
 //       {
	//		sprintf( sPdfName,"%s\\HKXB2015%d.pdf", sPath.c_str(), i );
	//		sprintf( sXmlName,"%s\\HKXB2015%d.xml", sPath.c_str(), i );
	//		sprintf( sPdfName10,"%s\\HKXB2015%d10.pdf", sPath.c_str(), i );
	//		sprintf( sPdfName22,"%s\\HKXB2015%d22.pdf", sPath.c_str(), i );
	//		sprintf( sPdfName23,"%s\\HKXB2015%d23.pdf", sPath.c_str(), i );
	//		sprintf( sPdfName26,"%s\\HKXB2015%d26.pdf", sPath.c_str(), i );
 //       }
	//	CopyFile(sFileName.c_str(),sPdfName,false);
	//	CopyFile(sFileNameXml.c_str(),sXmlName,false);
	//	CopyFile(sFileName10.c_str(),sPdfName10,false);
	//	CopyFile(sFileName22.c_str(),sPdfName22,false);
	//	CopyFile(sFileName23.c_str(),sPdfName23,false);
	//	CopyFile(sFileName26.c_str(),sPdfName26,false);
	//}
	////end
	//return 0;
	map<hfsstring,int>	m_mapCjfdCode;
	map<hfsstring,int>	m_mapCdmdCode;
	map<hfsstring,int>	m_mapCpmdCode;
	map<hfsstring,int>	m_mapCheckCpmd;//验证会议时，做排除用，比如报纸ccnd，这样可以少查kbase
	vector<string> items;
	int iCount= 0;
	items.clear();
	CUtility::Str()->CStrToStrings(taskData->szCjfdCode,items,',');
	iCount = items.size();

	for (int i=0; i < iCount; ++i)
	{
		m_mapCjfdCode.insert(make_pair(items[i],1));
	}
	items.clear();
	CUtility::Str()->CStrToStrings(taskData->szCdmdCode,items,',');
	iCount = items.size();

	for (int i=0; i < iCount; ++i)
	{
		m_mapCdmdCode.insert(make_pair(items[i],1));
	}
	items.clear();
	CUtility::Str()->CStrToStrings(taskData->szCpmdCode,items,',');
	iCount = items.size();

	for (int i=0; i < iCount; ++i)
	{
		m_mapCpmdCode.insert(make_pair(items[i],1));
	}

	items.clear();
	CUtility::Str()->CStrToStrings(taskData->szCheckCpmd,items,',');
	iCount = items.size();

	for (int i=0; i < iCount; ++i)
	{
		m_mapCheckCpmd.insert(make_pair(items[i],1));
	}

	vector<string>strDownFileList;

	CFolder f;
	vector<string>strFileList;
	vector<string>::iterator it2;
	f.GetFilesList( taskData->szLocPath, "*.zip", strFileList );

	SetCurrentDirectory( taskData->szLocDirectory );
	CUtility::Logger()->d("unzip file path[%s].",taskData->szLocDirectory);
	string strFilePathCurrent = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
		HZIP hz = OpenZip( (char *)it2->c_str(),0,ZIP_FILENAME );
		ZIPENTRY ze; 
		GetZipItem( hz, -1, &ze ); 
		int numitems = ze.index;
		for( int i = 0; i < numitems; i ++ )
		{ 
			GetZipItem( hz, i, &ze );
			SetCurrentDirectory( taskData->szLocDirectory );
			strFilePathCurrent = taskData->szLocDirectory;
			strFilePathCurrent += "\\";
			strFilePathCurrent += ze.name;
			_chmod( strFilePathCurrent.c_str(), _S_IWRITE );
			::DeleteFile( strFilePathCurrent.c_str() );
			if( UnzipItem( hz, i, ze.name, 0, ZIP_FILENAME ) == ZR_OK )
			{
				strDownFileList.push_back( ze.name );
				//_chmod( ze.name, _S_IWRITE );
				//DeleteFile( ze.name );
			}
			else
			{
				break;
			}
		}
		CloseZip( hz );

	}

	HS_MapStrToInt hashCjfd;
	hashCjfd.AllocMemoryPool();
	hashCjfd.InitHashTable(4096);
	HS_MapStrToInt hashCdmd;
	hashCdmd.AllocMemoryPool();
	hashCdmd.InitHashTable(4096);
	HS_MapStrToInt hashCpmd;
	hashCpmd.AllocMemoryPool();
	hashCpmd.InitHashTable(4096);

	map<hfsstring,int> mapCjfd;
	map<hfsstring,int> mapCdmd;
	map<hfsstring,int> mapCpmd;

	map<hfsstring,hfsstring> mapCjfdCode;
	map<hfsstring,hfsstring> mapCdmdCode;
	map<hfsstring,hfsstring> mapCpmdCode;
	int nptrVal;

	char szKey[32];
	char szCode[5];

	char szLine[4096] = {0};
	items.clear();
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char szFileName[18];
	char szPubYear[5], szDownYear[5], szMon[3];
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "d20150401.txt" );

	//char szFile1[MAX_PATH];
	//char szFileBack1[MAX_PATH];
	//GetModuleFileName( NULL, szFile1, sizeof(szFile1) );
	//memcpy(szFileBack1,szFile1,sizeof(szFile1));
	//strcpy( strrchr(szFile1, '\\') + 1, "cjfd" );
	//strcat( szFile1, "0505" );
	//strcat( szFile1, ".txt" );
	//FILE *hFileCjfd05 = OpenRecFile( szFile1 );

	CUtility::Logger()->d("hashtable write begin.");
	for( vit = strDownFileList.begin(); vit != strDownFileList.end(); ++ vit )
	{
		try
		{
			CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());
			strncpy( szDownYear, vit->c_str() + 1, 4 );
			szDownYear[4] = '\0';
			strncpy( szMon, vit->c_str() + 5, 2 );
			szMon[2] = '\0';
			if( szMon[0] == '0' )
			{
				szMon[0] = szMon[1];
				szMon[1] = '\0';
			}

			SetCurrentDirectory( taskData->szLocDirectory );
			fp=fopen(vit->c_str(),"r");  
			if (!fp)
			{
				continue;
			}
			int i=0;  
			string sPreLine = "";
			string sCurLine = "";
			string sFileName = "";
			string::size_type   pos(0);     
			cChar=fgetc(fp);  
			while(!feof(fp))  
			{  
				//try
				//{
					if (cChar=='\r')
					{
						cChar=fgetc(fp);  
						continue;
					}
					if (cChar!='\n')
					{
						szLine[i]=cChar;  
					}
					else
					{
						szLine[i]='\0'; 
						sCurLine = szLine;
						if( (pos=sCurLine.find("\t"))!=string::npos )
						{
							if (pos >= 4)
							{
								memset(szCode,0,sizeof(szCode));
								memcpy(szCode,(sCurLine.substr(pos - 4,4)).c_str(),4);
								_strupr( szCode );
							}
						}

						pBegin = strchr( szLine, '\t' );
						if( !pBegin )
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}

						pBegin ++;

						pEnd = strrchr( szLine, '\t' );
						if( !pEnd )
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}
						*pEnd = '\0';

						strncpy( szFileName, pBegin, sizeof(szFileName) - 1 );
						_strupr( szFileName );
						sFileName = szFileName;
						if(strstr(sFileName.c_str()," ")){
							Trim(szFileName,' ');
						}
						if (sFileName.length() < 7)
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
							cChar=fgetc(fp);  
							continue;
						}
						if( IsCJFD( &m_mapCjfdCode, szFileName, szCode ))
						{
							//fwrite( szLine, 1, (int)strlen(szLine), hFileCjfd05 );
							//fwrite( "\n", 1, 1, hFileCjfd05 );
							memset( szKey, 0, sizeof(szKey) );
							strncpy( szKey, szFileName, sizeof(szFileName) - 1 );

							//cit = mapCjfdCode.find(szFileName);
							//if(cit == mapCjfdCode.end()){
								mapCjfdCode.insert(make_pair(szFileName,szCode));
							//}

							if( hashCjfd.Lookup( szKey, nptrVal ) )
							{
								hashCjfd.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCjfd.SetAt( szKey, 1 );
							}		
						}
						else if (IsCDMD( &m_mapCdmdCode, szFileName, szCode ))
						{
							memset( szKey, 0, sizeof(szKey) );
							strncpy( szKey, szFileName, sizeof(szFileName) - 1 );

							//cit = mapCdmdCode.find(szFileName);
							//if(cit == mapCdmdCode.end()){
								mapCdmdCode.insert(make_pair(szFileName,szCode));
							//}

							if( hashCdmd.Lookup( szKey, nptrVal ) )
							{
								hashCdmd.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCdmd.SetAt( szKey, 1 );
							}		
						}
						else if (IsCPFD( &m_mapCpmdCode, szFileName, szCode, &m_mapCheckCpmd ))
						{
							memset( szKey, 0, sizeof(szKey) );
							strncpy( szKey, szFileName, sizeof(szFileName) - 1 );

							//cit = mapCpmdCode.find(szFileName);
							//if(cit == mapCpmdCode.end()){
								mapCpmdCode.insert(make_pair(szFileName,szCode));
							//}

							if( hashCpmd.Lookup( szKey, nptrVal ) )
							{
								hashCpmd.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCpmd.SetAt( szKey, 1 );
							}		
						}

						sPreLine = szLine;
						bInsert = true;
					}
					if (!bInsert)
					{
						++i;  
					}
					else
					{
						i = 0;
						memset( szLine, 0, sizeof(szLine) );
						bInsert = false;
					}
					cChar=fgetc(fp);  
				//}
				//catch(exception ex)
				//{
				//	CUtility::Logger()->d("exception .");

				//}
			}  
			if (fp) fclose(fp);
			Sleep(500);
			strFilePath = taskData->szLocDirectory;
			strFilePath += "\\";
			strFilePath += *vit;
			_chmod( strFilePath.c_str(), _S_IWRITE );
			::DeleteFile( strFilePath.c_str() );
			//fclose(hFileCjfd05);
		}
		catch(exception ex)
		{
			CUtility::Logger()->d("exception happen.");
		}
	}
	CUtility::Logger()->d("hashtable write end.");

	int nRet = 0;
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20];
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szCjfdFileAdd[MAX_PATH];
	char szCdmdFileAdd[MAX_PATH];
	char szCpmdFileAdd[MAX_PATH];
	char szCjfdFileUpdate[MAX_PATH];
	char szCdmdFileUpdate[MAX_PATH];
	char szCpmdFileUpdate[MAX_PATH];

	char szFile[MAX_PATH];
	char szFileBack[MAX_PATH];
	GetModuleFileName( NULL, szFile, sizeof(szFile) );
	memcpy(szFileBack,szFile,sizeof(szFile));
	strcpy( strrchr(szFile, '\\') + 1, "cjfdadd" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCjfdFileAdd,szFile,sizeof(szFile));

	memcpy(szFile,szFileBack,sizeof(szFileBack));
	strcpy( strrchr(szFile, '\\') + 1, "cjfdUpdate" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCjfdFileUpdate,szFile,sizeof(szFile));

	memcpy(szFile,szFileBack,sizeof(szFileBack));
	strcpy( strrchr(szFile, '\\') + 1, "cdmdadd" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCdmdFileAdd,szFile,sizeof(szFile));

	memcpy(szFile,szFileBack,sizeof(szFileBack));
	strcpy( strrchr(szFile, '\\') + 1, "cdmdupdate" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCdmdFileUpdate,szFile,sizeof(szFile));

	memcpy(szFile,szFileBack,sizeof(szFileBack));
	strcpy( strrchr(szFile, '\\') + 1, "cpmdadd" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCpmdFileAdd,szFile,sizeof(szFile));

	memcpy(szFile,szFileBack,sizeof(szFileBack));
	strcpy( strrchr(szFile, '\\') + 1, "cpmdupdate" );
	strcat( szFile, taskData->szYearMonth );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	memcpy(szCpmdFileUpdate,szFile,sizeof(szFile));

	::DeleteFile( szCjfdFileAdd );
	::DeleteFile( szCdmdFileAdd );
	::DeleteFile( szCpmdFileAdd );
	::DeleteFile( szCjfdFileUpdate );
	::DeleteFile( szCdmdFileUpdate );
	::DeleteFile( szCpmdFileUpdate );

	FILE *hFileCjfdAdd = OpenRecFile( szCjfdFileAdd );
	FILE *hFileCdmdAdd = OpenRecFile( szCdmdFileAdd );
	FILE *hFileCpfdAdd = OpenRecFile( szCpmdFileAdd );

	FILE *hFileCjfdUpdate = OpenRecFile( szCjfdFileUpdate );
	FILE *hFileCdmdUpdate = OpenRecFile( szCdmdFileUpdate );
	FILE *hFileCpfdUpdate = OpenRecFile( szCpmdFileUpdate );

	char szBuff[64];
	char szID[16];
	char szMonth[16];
	HITEM_POSITION pos;
	char *key;
	int iValue = 0;
	iValue = _atoi64(szMon);
	switch (iValue)
	{
		case 1:
			strcpy(szMonth,"一月下载");
			break;
		case 2:
			strcpy(szMonth,"二月下载");
			break;
		case 3:
			strcpy(szMonth,"三月下载");
			break;
		case 4:
			strcpy(szMonth,"四月下载");
			break;
		case 5:
			strcpy(szMonth,"五月下载");
			break;
		case 6:
			strcpy(szMonth,"六月下载");
			break;
		case 7:
			strcpy(szMonth,"七月下载");
			break;
		case 8:
			strcpy(szMonth,"八月下载");
			break;
		case 9:
			strcpy(szMonth,"九月下载");
			break;
		case 10:
			strcpy(szMonth,"十月下载");
			break;
		case 11:
			strcpy(szMonth,"十一月下载");
			break;
		case 12:
			strcpy(szMonth,"十二月下载");
			break;
	}
	char szOneKey[32] = "";
	iValue = _atoi64(szDownYear);
	int iSize = 0;
	string strUniqCode = "文件名";
	CUtility::Logger()->d("GetMap[%s] begin",taskData->cjfdtablename);
	nRet = GetMap(taskData->cjfdtablename, strUniqCode.c_str(), mapCjfd, iValue);
	iSize = mapCjfd.size();
	CUtility::Logger()->d("GetMap[%s] size[%d]Result[%d]",taskData->cjfdtablename,iSize,nRet);
	strUniqCode = "文件名年";
	CUtility::Logger()->d("GetMap[%s] begin",taskData->cdmdtablename);
	nRet = GetMap(taskData->cdmdtablename, strUniqCode.c_str(), mapCdmd, iValue);
	iSize = mapCdmd.size();
	CUtility::Logger()->d("GetMap[%s] size[%d]Result[%d]",taskData->cdmdtablename,iSize,nRet);
	CUtility::Logger()->d("GetMap[%s] begin",taskData->cpmdtablename);
	nRet = GetMap(taskData->cpmdtablename, strUniqCode.c_str(), mapCpmd, iValue);
	iSize = mapCpmd.size();
	CUtility::Logger()->d("GetMap[%s] size[%d]Result[%d]",taskData->cpmdtablename,iSize,nRet);
	
	std::map<hfsstring,int>::iterator mit;
	CUtility::Logger()->d("hashtable to cjfdfile start.");
	for( pos = hashCjfd.GetStartPosition(); pos != NULL; )
	{
		hashCjfd.GetNextAssoc( pos, key, nptrVal );
		
		memset( szFileName, 0, sizeof(szFileName) );
		//pBegin = strchr( key, ',' );
		//strncpy( szFileName, key, pBegin - key);
		strcpy(szFileName,key);
		memcpy(szOneKey, szFileName, sizeof(szFileName));
		strcat(szOneKey,"-");
		strcat(szOneKey,szDownYear);
		memset( szCode, 0, 5 );
		//strncpy( szCode, pBegin + 1, 4 );
		cit = mapCjfdCode.find(szFileName);
		if(cit != mapCjfdCode.end())
		{
			memcpy(szCode, cit->second.c_str(),4);
		}
		mit = mapCjfd.find(szFileName);
		if(mit == mapCjfd.end()){
			WriteRecFile( hFileCjfdAdd, "文件名", szFileName, false );
			WriteRecFile( hFileCjfdAdd, "库代码", szCode, true );
			WriteRecFile( hFileCjfdAdd, "年", szDownYear, true );
			WriteRecFile( hFileCjfdAdd, szMonth, nptrVal, true );
		}
		else
		{
			WriteRecFile( hFileCjfdUpdate, "文件名", szFileName, false );
			WriteRecFile( hFileCjfdUpdate, szMonth, nptrVal, true );
		}
	}
	mapCjfd.clear();
	mapCjfdCode.clear();
	CUtility::Logger()->d("hashtable to cjfdfile end.");
	CUtility::Logger()->d("hashtable to cdmdfile start.");
	for( pos = hashCdmd.GetStartPosition(); pos != NULL; )
	{
		hashCdmd.GetNextAssoc( pos, key, nptrVal );
		
		memset( szFileName, 0, sizeof(szFileName) );
		//pBegin = strchr( key, ',' );
		//strncpy( szFileName, key, pBegin - key);
		strcpy(szFileName,key);
		memcpy(szOneKey, szFileName, sizeof(szFileName));
		strcat(szOneKey,"-");
		strcat(szOneKey,szDownYear);
		memset( szCode, 0, 5 );
		//strncpy( szCode, pBegin + 1, 4 );
		cit = mapCdmdCode.find(szFileName);
		if(cit != mapCdmdCode.end())
		{
			memcpy(szCode, cit->second.c_str(),4);
		}

		mit = mapCdmd.find(szOneKey);
		if(mit == mapCdmd.end())
		{
			WriteRecFile( hFileCdmdAdd, "文件名", szFileName, false );
			WriteRecFile( hFileCdmdAdd, "库代码", szCode, true );
			WriteRecFile( hFileCdmdAdd, "年", szDownYear, true );
			WriteRecFile( hFileCdmdAdd, szMonth, nptrVal, true );
			WriteRecFile( hFileCdmdAdd, "文件名年", szOneKey, true );
		}
		else
		{
			WriteRecFile( hFileCdmdUpdate, "文件名年", szOneKey, false );
			WriteRecFile( hFileCdmdUpdate, szMonth, nptrVal, true );
		}
	}
	mapCdmd.clear();
	mapCdmdCode.clear();
	CUtility::Logger()->d("hashtable to cdmdfile end.");
	CUtility::Logger()->d("hashtable to cpmdfile start.");

	for( pos = hashCpmd.GetStartPosition(); pos != NULL; )
	{
		hashCpmd.GetNextAssoc( pos, key, nptrVal );
		
		memset( szFileName, 0, sizeof(szFileName) );
		//pBegin = strchr( key, ',' );
		//strncpy( szFileName, key, pBegin - key);
		strcpy(szFileName,key);
		memcpy(szOneKey, szFileName, sizeof(szFileName));
		strcat(szOneKey,"-");
		strcat(szOneKey,szDownYear);
		memset( szCode, 0, 5 );
		//strncpy( szCode, pBegin + 1, 4 );
		cit = mapCpmdCode.find(szFileName);
		if(cit != mapCpmdCode.end())
		{
			memcpy(szCode, cit->second.c_str(),4);
		}

		mit = mapCpmd.find(szOneKey);
		if(mit == mapCpmd.end())
		{
			WriteRecFile( hFileCpfdAdd, "文件名", szFileName, false );
			WriteRecFile( hFileCpfdAdd, "库代码", szCode, true );
			WriteRecFile( hFileCpfdAdd, "年", szDownYear, true );
			WriteRecFile( hFileCpfdAdd, szMonth, nptrVal, true );
			WriteRecFile( hFileCpfdAdd, "文件名年", szOneKey, true );
		}
		else
		{
			WriteRecFile( hFileCpfdUpdate, "文件名年", szOneKey, false );
			WriteRecFile( hFileCpfdUpdate, szMonth, nptrVal, true );
		}
	}
	mapCpmd.clear();
	mapCpmdCode.clear();	
	CUtility::Logger()->d("hashtable to cpmdfile end.");
	fclose( hFileCjfdAdd );
	fclose( hFileCdmdAdd );
	fclose( hFileCpfdAdd );
	fclose( hFileCjfdUpdate );
	fclose( hFileCdmdUpdate );
	fclose( hFileCpfdUpdate );
	hashCjfd.RemoveAll();
	hashCdmd.RemoveAll();
	hashCpmd.RemoveAll();
	hashCjfd.FreeMemoryPool();
	hashCdmd.FreeMemoryPool();
	hashCpmd.FreeMemoryPool();

	//iValue = _atoi64(szMon);
	strUniqCode = "文件名";
	CUtility::Logger()->d("update table [%s] start.",taskData->cjfdtablename);
	nRet = UpdateTableTrue( taskData->cjfdtablename, szCjfdFileUpdate, strUniqCode.c_str() );
	CUtility::Logger()->d("update table [%s] end[%d].",taskData->cjfdtablename,nRet);

	strUniqCode = "文件名年";
	CUtility::Logger()->d("update table [%s] start.",taskData->cdmdtablename);
	nRet = UpdateTableTrue( taskData->cdmdtablename, szCdmdFileUpdate, strUniqCode.c_str() );
	CUtility::Logger()->d("update table [%s] end[%d].",taskData->cdmdtablename,nRet);

	CUtility::Logger()->d("update table [%s] start.",taskData->cpmdtablename);
	nRet = UpdateTableTrue( taskData->cpmdtablename, szCpmdFileUpdate, strUniqCode.c_str() );
	CUtility::Logger()->d("update table [%s] end[%d].",taskData->cpmdtablename,nRet);

	CUtility::Logger()->d("add table [%s] start.",taskData->cjfdtablename);
	nRet = AppendTable( taskData->cjfdtablename, szCjfdFileAdd );
	CUtility::Logger()->d("add table [%s] end[%d].",taskData->cjfdtablename,nRet);

	CUtility::Logger()->d("add table [%s] start.",taskData->cdmdtablename);
	nRet = AppendTable( taskData->cdmdtablename, szCdmdFileAdd );
	CUtility::Logger()->d("add table [%s] end[%d].",taskData->cdmdtablename,nRet);

	CUtility::Logger()->d("add table [%s] start.",taskData->cpmdtablename);
	nRet = AppendTable( taskData->cpmdtablename, szCpmdFileAdd );
	CUtility::Logger()->d("add table [%s] end[%d].",taskData->cpmdtablename,nRet);

	iValue = _atoi64(szMon);
	//if (iValue == 12)
	{
		nRet = UpdateFiledValue( taskData->cjfdtablename );
		CUtility::Logger()->d("update table [%s] fileid result[%d].",taskData->cjfdtablename,nRet);
		iValue = _atoi64(szDownYear);
		nRet = UpdateFiledValueYear( taskData->cdmdtablename, iValue );
		CUtility::Logger()->d("update table [%s] year [%d]fileid result[%d].", taskData->cdmdtablename, iValue, nRet);
		nRet = UpdateFiledValueYear( taskData->cpmdtablename, iValue );
		CUtility::Logger()->d("update table [%s] year [%d]fileid result[%d].", taskData->cpmdtablename, iValue, nRet);
	}
	//nRet = SortTable( taskData->nStatType);
	//AppendTable;

	return nRet;
}

int CKBConn::LoadCJFDBaseinfo(map<string,string>&pykmMap)
{
	int nRet = 0;
	char szSql[1024];
	sprintf( szSql, "select PYKM,C_NAME from %s", "cjfdbaseinfo" );
	TPI_HRECORDSET hSet=NULL;
	if( IsConnected() >= _ERR_SUCCESS){
		hSet= TPI_OpenRecordSet(m_hConn,szSql,&nRet);
		if( !hSet || nRet < 0 )
		{
			char szMsg[1024];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, nRet );
			//WriteLog( szMsg );
		
			return nRet;
		}
	}

	nRet = TPI_GetRecordSetCount( hSet );
	TPI_SetRecordCount( hSet, 500 );
	TPI_MoveFirst( hSet );

	long nLen;
	char szPYKM[16];
	char szName[1024];
	for( int i = 0; i < nRet; i ++ )
	{
		nLen = (int)sizeof(szPYKM);
		memset( szPYKM, 0, nLen );
		nLen = TPI_GetFieldValueByName( hSet, "PYKM", szPYKM, nLen );
		::strupr( szPYKM );

		nLen = (int)sizeof(szName);
		memset( szName, 0, nLen );
		nLen = TPI_GetFieldValueByName( hSet, "C_NAME", szName, nLen );

		//sprintf( szSql, "update DOWN_REF0511 set 刊名='%s' where PYKM='%s'\n go", szName, szPYKM );
		//WriteLog( szSql );

		pykmMap.insert( std::make_pair( szPYKM, szName ) );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet( hSet );

	return nRet;	
}

bool CKBConn::IsCJFD(map<hfsstring,int> * code, const char *pFileName, const char *pCode)
{
	map<hfsstring,int>::iterator it;
	bool bRet = false;
	if( !pFileName )
		return bRet;

	int nLen = (int)strlen( pFileName );
	if( nLen != 11 && nLen != 13 )
		return bRet;
	if (code)
	{
		it = code->find(pCode);
		bRet = true;
		if( it != code->end()){
			return bRet;
		}
	}
	for( int i = 0; i < 8; i ++ )
	{
		if( i < 4 )
		{
			if( pFileName[i] < 'A' || pFileName[i] > 'Z' )
			{
				bRet = false;
				break;
			}
		}
		else
		{
			if( pFileName[i] < '0' || pFileName[i] > '9' )
			{
				if (i != 7)
				{
					bRet = false;
				}
				else
				{
					if (pFileName[i] != '.')
					{
						bRet = false;
					}
				}
				break;
			}
		}
	}

	return bRet;
}

bool CKBConn::IsCDMD(map<hfsstring,int> * code, const char *pFileName, const char *pCode)
{
	map<hfsstring,int>::iterator it;
	if( !pFileName )
		return false;

	it = code->find(pCode);
	if( it != code->end()){
		return true;
	}

	string sTemp = "";
	char szTemp[MAX_FILE_ID];
	strcpy_s(szTemp,sizeof(szTemp),pFileName);
	::strupr(szTemp);
	sTemp = szTemp;
	int nLen = sTemp.length();
	for( int i = 0; i < nLen - 3 ; i ++ )
	{
		if( pFileName[i] < '0' || pFileName[i] > '9' )
		{
			return false;
		}
	}
	if (sTemp.substr(nLen - 3,3)==".NH")
	{
		return true;
	}

	return false;
}

bool CKBConn::IsCPFD(map<hfsstring,int> * code, const char *pFileName, const char *pCode,map<hfsstring,int> *checkCode)
{
	map<hfsstring,int>::iterator it;
	if( !pFileName )
		return false;

	it = code->find(pCode);
	if( it != code->end()){
		return true;
	}

	int nLen = (int)strlen( pFileName );
	if( nLen != 16 )
		return false;

	it = checkCode->find(pCode);
	if( it != checkCode->end()){
		return false;
	}

	string sTemp = "";
	char szTemp[MAX_FILE_ID];
	memset(szTemp, 0, sizeof(szTemp));
	strcpy_s(szTemp,sizeof(szTemp),pFileName);
	::strupr(szTemp);
	sTemp = szTemp;
	sTemp = sTemp.substr(0,14);


	char szSql[1024];
	int nRet = 0;
	sprintf( szSql, "select count(*) from cpfd_ji,ipfd_ji where 论文集代码='%s'",sTemp.c_str());

	nRet = GetTop1Value( szSql );
	if( nRet > 0 )
	{
		return true;
	}
	//select 论文集代码 from cpfd_ji,ipfd_ji

	return false;
}

int CKBConn::GetTop1Value(const char *pSql)
{
	int nRet = 0;

	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[ sql %s]",pSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,pSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", pSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	if( TPI_GetRecordSetCount( hSet ) > 0 )
	{
		TPI_MoveFirst( hSet );

		long nLen = 64;
		char szBuff[64];
		memset( szBuff, 0, nLen );
		TPI_GetFieldValue( hSet, 0, szBuff, nLen );
		if( szBuff[0] != '\0' )
			nRet = _atoi64( szBuff );
	}
	TPI_CloseRecordSet( hSet );

	return nRet;
}

bool CKBConn::GetPubYear(const char *pFileName, char *szPubYear)
{
	bool bRet = true;
	memset( szPubYear, 0, 5 );

	int nLen = (int)strlen(pFileName);
	if( nLen == 13 )
		strncpy( szPubYear, pFileName + 4, 4 );
	else if( nLen == 11 )
	{
		strcpy( szPubYear, "199" );
		strncpy( szPubYear + 3, pFileName + 4, 1 );
	}
	else 
		bRet = false;


	int nYear = atoi( szPubYear );
	if( nYear < 1900 )
		bRet = false;

	return bRet;

}

int CKBConn::GetMonthDays(int year, int month)
{
	if(year < 0)
		return 0;
	switch(month){
		case 1:
		case 3:
		case 5:
		case 7:
		case 8:
		case 10:
		case 12:
			return 31;
			break;
		case 4:
		case 6:
		case 9:
		case 11:
			return 30;
			break;
		case 2:
			if((year % 400 == 0 ) || (year % 4 == 0 && year % 100 != 0)){
				return 29;
			}else{
				return 28;
			}
			break;
		default:
			return 0;
	}
	return 0;
}

int	CKBConn::GetLocalFiles(DOWN_STATE * taskData)
{
	WIN32_FIND_DATA findData;
	TCHAR strFilePath[MAX_PATH];
	lstrcpy(strFilePath,taskData->szLocPath);
	lstrcat(strFilePath,_T("\\*.*"));
	HANDLE hResult = ::FindFirstFile( strFilePath, &findData );
	if(FALSE == (hResult != INVALID_HANDLE_VALUE))
	{
		return 0; 
	}
	int nCountFiles = 0;
	string szFile = "";
	char szYearMonth[7] = {0};
	char szYearMonthDay[9] = {0};
	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cDay[9] = {0};
	sprintf(cDay,"%04d%02d%02d",systime.wYear,systime.wMonth,systime.wDay);

	while(::FindNextFile(hResult,&findData))
	{
		if(findData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY)
		{
		}
		else
		{
			szFile=findData.cFileName;
			strncpy( szYearMonth, findData.cFileName + 1, 6 );
			strncpy( szYearMonthDay, findData.cFileName + 1, 8 );
			if (strcmp(szYearMonth,taskData->szYearMonth) == 0)
			{
				//当天日期不进入下载列表
				if (strcmp(szYearMonthDay,cDay) != 0)
				{
					++nCountFiles;
				}
			}
		}
	}
	::FindClose( hResult );
	return nCountFiles;
}

int	CKBConn::GetDifferFile(DOWN_STATE * taskData)
{
	string str,szFile,szFtpInfo;
	WIN32_FIND_DATA findData;
	HINTERNET hFind;
	char szAppName[256];
	strcpy(szAppName,"ColumnCounter-name");
	HINTERNET hInetSession=InternetOpen(szAppName,INTERNET_OPEN_TYPE_PRECONFIG,NULL,NULL,0);
	HINTERNET hFtpConn=InternetConnect(hInetSession,taskData->szServer,taskData->nftpPort,
		taskData->szUser,taskData->szPwd,INTERNET_SERVICE_FTP,INTERNET_FLAG_PASSIVE,0);
	if(!hFtpConn)
	{
		InternetCloseHandle(hInetSession);
		::Sleep(10);
		return 0L;
	}
	DWORD dwLength=MAX_PATH;
	char szFtpDirectory[MAX_PATH]={0};
	strcpy(szFtpDirectory,taskData->szServerDirectory);
	if(szFtpDirectory!=0)
		FtpSetCurrentDirectory(hFtpConn,szFtpDirectory);
	FtpGetCurrentDirectory(hFtpConn,szFtpDirectory,&dwLength);
	::SetCurrentDirectory(taskData->szLocPath);
	if(!(hFind=FtpFindFirstFile(hFtpConn,_T("*"),&findData,0,0)))
	{
		if (GetLastError()  == ERROR_NO_MORE_FILES) 
		{
		}
		::Sleep(10);
		InternetCloseHandle(hFtpConn);
		InternetCloseHandle(hInetSession);
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
			if (strcmp(szYearMonth,taskData->szYearMonth) == 0)
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

	TCHAR strFilePath[MAX_PATH];
	lstrcpy(strFilePath,taskData->szLocPath);
	lstrcat(strFilePath,_T("\\*.*"));
	HANDLE hResult = ::FindFirstFile( strFilePath, &findData );
	if(FALSE == (hResult != INVALID_HANDLE_VALUE))
	{
		return 0; 
	}
	int nCountFiles = 0;

	map<hfsstring,DWORD>::iterator it;
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
					++nCountFiles;
				}
			}
		}
	}
	::FindClose( hResult );
	return nCountFiles;
}

int CKBConn::StatZt(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	//char pTable[255] = {0};
	//strcpy(pTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, pTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	long long lCount = 0;
	ST_ITEMZT item;
	char szSql[MAX_SQL], szBuff[64];
	vector<ST_ZJCLS *>::iterator it;
	std::map<hfsstring,string>::iterator itt;
	std::map<hfsstring,int>::iterator iti;
	string strZtCode = "";
	string strAuthorCode = "";
	vector<pair<string,int>> tmpVector;  
	vector<pair<string,int>> tVector;  
	vector<string> items;
	string::size_type   pos(0);     
	string strCode = "";
	string strPykm = "";
	string strResult = "";
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	char cTime[64] = {0};
	nType = taskData->nStatType;
	vector<ST_ZJCLS *> m_ALLZJCls;
	LoadAllZJCLS(m_ALLZJCls);
	for( it =  m_ALLZJCls.begin(); it !=  m_ALLZJCls.end(); ++ it )
	{
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("loop start time.[%s]",cTime);
		}
		CUtility::Logger()->d("doing.[%s]",(*it)->szCode);
		nRet = 0;
		sprintf( szSql, "select 年 as Nian,count(*) as counts from  %s where 专题子栏目代码=%s? group by (年,'date') order by 年 asc", pTable, (*it)->szCode );

		m_ErrCode = IsConnected();
		TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]",pTable,szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);

			CloseRecFile( hFile );
			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("TYPE[%d]执行[%s]，结果集为0",nType,szSql);
		}
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		TPI_MoveFirst( hSet );

		if( m_ErrCode < 0 )
		{
			CloseRecFile( hFile );
			return m_ErrCode;
		}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		char t[64] = {0};
		int nCounts = 0;
		string strYear = "";
		string strCounts = "";
		for( int i = 0; i < nRecordCount; i ++ )
		{
			//memset( &item, 0, sizeof(ST_ITEMZT) );
			
			memset(t,0,64);
			m_ErrCode = TPI_GetFieldValueByName(hSet,"Nian",t,64);	
			if(t[0] == '\0'){
				TPI_MoveNext(hSet);
				continue;
			}
			else
			{
				strYear = t;
			}
			memset(t,0,64);
			m_ErrCode = TPI_GetFieldValueByName(hSet,"Counts",t,64);	
			if(t[0] == '\0'){
				TPI_MoveNext(hSet);
				continue;
			}
			else
			{
				strCounts = t;
				nCounts = _atoi64(t);
			}
			//Code
			//strcpy(item.szCode,(*it)->szCode);
			//Name
			//strcpy(item.szName,(*it)->szName);

			fwrite( pTable, 1, (int)strlen(pTable), hFile );
			fwrite( ",", 1, 1, hFile );
			fwrite( (*it)->szCode, 1, (int)strlen((*it)->szCode), hFile );
			fwrite( ",", 1, 1, hFile );
			fwrite( (*it)->szName, 1, (int)strlen((*it)->szName), hFile );
			fwrite( ",", 1, 1, hFile );
			fwrite( strYear.c_str(), 1, (int)strlen(strYear.c_str()), hFile );
			fwrite( ",", 1, 1, hFile );
			fwrite( strCounts.c_str(), 1, (int)strlen(strCounts.c_str()), hFile );
			fwrite( "\n", 1, 1, hFile );
			lCount += nCounts;
			TPI_MoveNext(hSet);
		}
		string strCount = "";
		memset(t,0,sizeof(t));
		itoa(lCount,t,10);
		fwrite( pTable, 1, (int)strlen(pTable), hFile );
		fwrite( ",", 1, 1, hFile );
		fwrite( (*it)->szCode, 1, (int)strlen((*it)->szCode), hFile );
		fwrite( ",", 1, 1, hFile );
		fwrite( (*it)->szName, 1, (int)strlen((*it)->szName), hFile );
		fwrite( ",", 1, 1, hFile );
		fwrite( "总计", 1, (int)strlen("总计"), hFile );
		fwrite( ",", 1, 1, hFile );
		fwrite( t, 1, (int)strlen(t), hFile );
		fwrite( "\n", 1, 1, hFile );
		lCount = 0;
		TPI_CloseRecordSet(hSet);
		::fflush(hFile);
	}

	CloseRecFile( hFile );

	if( m_ErrCode >= 0 && nRecordCount > 0 )
	{
		nErrCode = nRecordCount;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nRecordCount[%d]",nRecordCount);
	}

	return nErrCode;

}

hfsbool		CKBConn::LoadAllZJCLS(vector<ST_ZJCLS*> &m_AllZJCls)
{

	int m_ErrCode = IsConnected();

	char szSql[128] = {0};
	sprintf( szSql, "select SYS_FLD_CLASS_CODE,SYS_FLD_CLASS_NAME from  %s", "zjcls" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	m_ErrCode  = TPI_GetRecordSetCount( hSet );

	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < m_ErrCode; i ++ )
	{
		ST_ZJCLS *item = new ST_ZJCLS;
		memset( item, 0, sizeof(ST_ZJCLS) );

		nLen = (int)sizeof(item->szCode);
		TPI_GetFieldValue( hSet, 0, item->szCode, nLen );


		nLen = (int)sizeof(item->szName);
		TPI_GetFieldValue( hSet, 1, item->szName, nLen );

		m_AllZJCls.push_back( item );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

void CKBConn::InsertLine()
{
    FILE   *fp;  
    FILE   *fp2;  
	string path = "E:\\iis\\cjfdtotal2014-09-12.txt";
	string path2 = "E:\\iis\\cjfdtotal2014-09-15.txt";
	fp2 = fopen(path2.c_str(),"w");
	fp = fopen(path.c_str(),"r");  
    int i = 0;  
	char line[255] = {0};
	char cChar=0;  
	string strLine = "";
	long long lCount = 0;
	string strNumber = "";
	string strValue = "";
	string strName = "";
	string strName2= "";
	string strTable="";
	string strCode="";
	string strCode2="";
	vector<string> items;
	cChar=fgetc(fp);  
	bool bInsert = false;
	char t[64] = {0};
    while(!feof(fp))  
    {  
		if (cChar=='\r')
		{
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			items.clear();
			CUtility::Str()->CStrToStrings(line,items,',');
			int iCount = items.size();
			if (iCount > 3)
			{
				strTable = items[0];
				strCode = items[1];
				strName = items[2];
				strValue = items[4];
				if ((strName == strName2)||(strName2==""))
				{
					lCount += _atoi64(strValue.c_str());
				}
				else
				{
					memset(t,0,sizeof(t));
					itoa(lCount,t,10);
					fwrite( strTable.c_str(), 1, (int)strlen(strTable.c_str()), fp2 );
					fwrite( ",", 1, 1, fp2 );
					fwrite( strCode2.c_str(), 1, (int)strlen(strCode2.c_str()), fp2 );
					fwrite( ",", 1, 1, fp2 );
					fwrite( strName2.c_str(), 1, (int)strlen(strName2.c_str()), fp2 );
					fwrite( ",", 1, 1, fp2 );
					fwrite( "总计", 1, (int)strlen("总计"), fp2 );
					fwrite( ",", 1, 1, fp2 );
					fwrite( t, 1, (int)strlen(t), fp2 );
					fwrite( "\n", 1, 1, fp2 );
					lCount = 0;
					lCount += _atoi64(strValue.c_str());
				}
			}
			fwrite( line, 1, (int)strlen(line), fp2 );
			fwrite( "\n", 1, 1, fp2 );
			bInsert = true;
			fflush(fp2);
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
			strName2 = strName;
			strCode2 = strCode;
		}
        cChar=fgetc(fp);  
    }  
	memset(t,0,sizeof(t));
	itoa(lCount,t,10);
	fwrite( strTable.c_str(), 1, (int)strlen(strTable.c_str()), fp2 );
	fwrite( ",", 1, 1, fp2 );
	fwrite( strCode2.c_str(), 1, (int)strlen(strCode2.c_str()), fp2 );
	fwrite( ",", 1, 1, fp2 );
	fwrite( strName2.c_str(), 1, (int)strlen(strName2.c_str()), fp2 );
	fwrite( ",", 1, 1, fp2 );
	fwrite( "总计", 1, (int)strlen("总计"), fp2 );
	fwrite( ",", 1, 1, fp2 );
	fwrite( t, 1, (int)strlen(t), fp2 );
	fwrite( "\n", 1, 1, fp2 );
	fclose(fp);
	fclose(fp2);

}

int CKBConn::LoadFieldInfo(const char * szSql, std::vector<string>&vCodeUniqCode)
{
	int nRet = 0;
	TPI_HRECORDSET hSet=NULL;
	if( IsConnected() >= _ERR_SUCCESS){
		hSet= TPI_OpenRecordSet(m_hConn,szSql,&nRet);
		if( !hSet || nRet < 0 )
		{
			char szMsg[1024];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, nRet );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			return nRet;
		}
	}

	nRet = TPI_GetRecordSetCount( hSet );
	TPI_SetRecordCount( hSet, 100 );
	TPI_MoveFirst( hSet );
	string strCodeUniqCode = "";
	long nLen;
	char szCode[1024];
	char szUniqCode[64];
	for( int i = 0; i < nRet; i ++ )
	{
		nLen = (int)sizeof(szCode);
		memset( szCode, 0, nLen );
		nLen = TPI_GetFieldValueByName( hSet, "Code", szCode, nLen );
		//::strupr( szCode );

		nLen = (int)sizeof(szUniqCode);
		memset( szUniqCode, 0, nLen );
		nLen = TPI_GetFieldValueByName( hSet, "UniqCode", szUniqCode, nLen );
		strCodeUniqCode = szCode;
		strCodeUniqCode += ",";
		strCodeUniqCode += szUniqCode;
		vCodeUniqCode.push_back(strCodeUniqCode);

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet( hSet );

	return nRet;	
}

int CKBConn::StatColumn(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	char szTable[255] = {0};
	strcpy(szTable,taskData->tablename);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, szTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	long long lCount = 0;
	ST_ITEMZT item;
	char szSql[MAX_SQL], szBuff[256];
	vector<ST_PYKM *>::iterator it;
	std::map<hfsstring,string>::iterator itt;
	std::map<hfsstring,int>::iterator iti;
	string strZtCode = "";
	string strAuthorCode = "";
	vector<pair<string,int>> tmpVector;  
	vector<pair<string,int>> tVector;  
	vector<string> items;
	string::size_type   pos(0);     
	string strPykm = "";
	string strName = "";
	string strColuHier = "";
	string strResult = "";
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	char cTime[64] = {0};
	nType = taskData->nStatType;
	vector<ST_PYKM *> m_ALLPykms;
	LoadPykm(m_ALLPykms);
	for( it =  m_ALLPykms.begin(); it !=  m_ALLPykms.end(); ++ it )
	{
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("loop start time.[%s]",cTime);
		}
		CUtility::Logger()->d("doing.[%s]",(*it)->szPykm);
		nRet = 0;
		sprintf( szSql, "select 拼音刊名, 中文刊名, %s from  cjfdtotal where 拼音刊名=%s? group by %s", pField, (*it)->szPykm, pCodeName );

		m_ErrCode = IsConnected();
		TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
		{
			CUtility::Logger()->d("超时 sql info.[在Exec sql %s]",szSql);
			Sleep( 1000 );
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);

			CloseRecFile( hFile );
			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("TYPE[%d]执行[%s]，结果集为0",nType,szSql);
		}
		if (nRecordCount > GETRECORDNUM*2)
		{
			m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		}
		TPI_MoveFirst( hSet );

		//if( m_ErrCode < 0 )
		//{
		//	CloseRecFile( hFile );
		//	return m_ErrCode;
		//}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		char t[256] = {0};
		int nCounts = 0;
		string strYear = "";
		string strCounts = "";
		for( int i = 0; i < nRecordCount; i ++ )
		{
			//memset( &item, 0, sizeof(ST_ITEMZT) );
			
			memset(t,0,256);
			m_ErrCode = TPI_GetFieldValueByName(hSet,"拼音刊名",t,256);	
			if(t[0] == '\0'){
				TPI_MoveNext(hSet);
				continue;
			}
			else
			{
				strPykm = t;
			}
			memset(t,0,256);
			m_ErrCode = TPI_GetFieldValueByName(hSet,"中文刊名",t,256);	
			if(t[0] == '\0'){
				TPI_MoveNext(hSet);
				continue;
			}
			else
			{
				strName = t;
			}
			memset(t,0,256);
			m_ErrCode = TPI_GetFieldValueByName(hSet,"期刊栏目层次",t,256);	
			if(t[0] == '\0'){
				TPI_MoveNext(hSet);
				continue;
			}
			else
			{
				strColuHier = t;
			}
			//Code
			//strcpy(item.szCode,(*it)->szCode);
			//Name
			//strcpy(item.szName,(*it)->szName);

			WriteRecFile( hFile, "拼音刊名", (*it)->szPykm, false );
			WriteRecFile( hFile, "中文刊名", (*it)->szName, true );
			WriteRecFile( hFile, "期刊栏目层次", strColuHier.c_str(), true );
			WriteRecFile( hFile, "更新日期", szDate, true );
			TPI_MoveNext(hSet);
		}
		TPI_CloseRecordSet(hSet);
		::fflush(hFile);
	}

	CloseRecFile( hFile );

	if( m_ErrCode >= 0 && nRecordCount > 0 )
	{
		nErrCode = nRecordCount;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nRecordCount[%d]",nRecordCount);
	}

	return nErrCode;

}

int CKBConn::StatColumn_NQ(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	char szTable[255] = {0};
	strcpy(szTable,pTable);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, szTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	long long lCount = 0;
	ST_ITEMNQ *item = new ST_ITEMNQ;
	char szSql[MAX_SQL], szBuff[256];
	vector<ST_PYKM *>::iterator it;
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	string::size_type   pos(0);  
	string strHier = "";
	char cTime[64] = {0};
	nType = taskData->nStatType;
	vector<ST_PYKM *> m_ALLPykms;
	LoadPykmHier(m_ALLPykms);
	CUtility::Logger()->d("层次表的记录数:[%d]",m_ALLPykms.size());
	for( it =  m_ALLPykms.begin(); it !=  m_ALLPykms.end(); ++ it )
	{
		//strHier = (*it)->szHier;
		//if (strHier != "科学跨越  长治久安_三化建设")
		//{
		//	continue;
		//}
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("loop start time.[%s]",cTime);
		}
		CUtility::Logger()->d("doing.[%s][%s]",(*it)->szPykm,(*it)->szHier);
		nRet = 0;
		strHier = (*it)->szHier;
		if(( (pos=strHier.find("'"))!=string::npos )&&( (pos=strHier.find("\""))!=string::npos ))
		{
			CUtility::Logger()->d("no do with 层次.[%s]",strHier);
			continue;
		}
		sprintf( szSql, "select THNAME,年,期,年期,count(*) from cjfdtotal where 拼音刊名='%s?' and %s=\"%s?\" group by %s", (*it)->szPykm, pField, (*it)->szHier, pCodeName );

		if( (pos=strHier.find("'"))!=string::npos )
		{
			sprintf( szSql, "select THNAME,年,期,年期,count(*) from cjfdtotal where 拼音刊名='%s?' and %s=\"%s?\" group by %s", (*it)->szPykm, pField, (*it)->szHier, pCodeName );
		}

		if( (pos=strHier.find("\""))!=string::npos )
		{
			sprintf( szSql, "select THNAME,年,期,年期,count(*) from cjfdtotal where 拼音刊名='%s?' and %s='%s?' group by %s", (*it)->szPykm, pField, (*it)->szHier, pCodeName );
		}
		TPI_HRECORDSET hSet = NULL;
		m_ErrCode = IsConnected();
		for (int i=1;i<5;i++)
		{
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
			{
				CUtility::Logger()->d("超时 sql info.[在Exec sql %s]",szSql);
				Sleep( 1000 );
				hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			}
			if (m_ErrCode != -6032)
			{
				break;
			}
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			Sleep(50000);
		}

		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);

			CloseRecFile( hFile );
			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("TYPE[%d]执行[%s]，结果集为0",nType,szSql);
		}
		if (nRecordCount > GETRECORDNUM*2)
		{
			m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		}
		TPI_MoveFirst( hSet );

		//if( m_ErrCode < 0 )
		//{
		//	CUtility::Logger()->d("TPI_SetRecordCount set exception[%d]",m_ErrCode);
		//	CloseRecFile( hFile );
		//	return m_ErrCode;
		//}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		char t[256] = {0};
		int nCounts = 0;
		string strYear = "";
		string strCounts = "";
		long long nLen;
		for( int i = 0; i < nRecordCount; i ++ )
		{
			memset( item, 0, sizeof(ST_ITEMNQ) );
			nLen = (int)sizeof(item->szThname);
			TPI_GetFieldValue( hSet, 0, item->szThname, nLen );
			nLen = (int)sizeof(item->szYear);
			TPI_GetFieldValue( hSet, 1, item->szYear, nLen );
			nLen = (int)sizeof(item->szPeriod);
			TPI_GetFieldValue( hSet, 2, item->szPeriod, nLen );
			nLen = (int)sizeof(item->szNQ);
			TPI_GetFieldValue( hSet, 3, item->szNQ, nLen );
			nLen = (int)sizeof(item->nCounts);
			TPI_GetFieldValue( hSet, 4, item->nCounts, nLen );

			try
			{
				WriteRecFile( hFile, "拼音刊名", (*it)->szPykm, false );
				WriteRecFile( hFile, "中文刊名", (*it)->szName, true );
				WriteRecFile( hFile, "期刊栏目层次",(*it)->szHier, true );
				WriteRecFile( hFile, "THNAME",item->szThname, true );
				WriteRecFile( hFile, "年",item->szYear, true );
				WriteRecFile( hFile, "期",item->szPeriod, true );
				WriteRecFile( hFile, "年期",item->szNQ, true );
				WriteRecFile( hFile, "本期该层次文献数",item->nCounts, true );
				WriteRecFile( hFile, "更新日期", szDate, true );
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
			TPI_MoveNext(hSet);
		}
		TPI_CloseRecordSet(hSet);
		::fflush(hFile);
	}

	CloseRecFile( hFile );
	if (item)
	{
		delete item;
	}
	if( m_ErrCode >= 0 && nRecordCount > 0 )
	{
		nErrCode = nRecordCount;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nRecordCount[%d]",nRecordCount);
	}

	return nErrCode;

}

int CKBConn::StatColumn_168(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere)
{
	char szTable[255] = {0};
	strcpy(szTable,pTable);
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20]={0};
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFileName[MAX_PATH];
	GetModuleFileName( NULL, szFileName, sizeof(szFileName) );
	strcpy( strrchr( szFileName, '\\' ) + 1, szTable );
	strcat( szFileName, szDate );
	strcat( szFileName, ".txt" );
	int nRecordCount = 0;
	FILE *hFile = OpenRecFile( szFileName );

	long nLen;
	int nErrCode, nCount = 0;
	int nCountRec = 0;
	long long lCount = 0;
	ST_ITEMHIER *item = new ST_ITEMHIER;
	char szSql[MAX_SQL], szBuff[256];
	vector<ST_PYKM *>::iterator it;
	int nType = 0;
	int nRet = 0;
	int nItemsCount = 0;
	int nAllResults = 0;
	SYSTEMTIME systime;
	string::size_type   pos(0);  
	string strHier = "";
	char cTime[64] = {0};
	nType = taskData->nStatType;
	vector<ST_PYKM *> m_ALLPykms;
	LoadPykmHierNq(m_ALLPykms);
	for( it =  m_ALLPykms.begin(); it !=  m_ALLPykms.end(); ++ it )
	{
		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("loop start time.[%s]",cTime);
		}
		CUtility::Logger()->d("doing.[%s]",(*it)->szPykm);
		nRet = 0;
		strHier = (*it)->szHier;
		if(( (pos=strHier.find("'"))!=string::npos )&&( (pos=strHier.find("\""))!=string::npos ))
		{
			CUtility::Logger()->d("no do with 层次.[%s]",strHier);
			continue;
		}
		sprintf( szSql, "select 中文刊名, 年, 期, 年期,专题代码, 子栏目代码,专题子栏目代码,子栏目名称 from  cjfdtotal where 拼音刊名='%s?' and %s=\"%s?\" and thname='%s?'", (*it)->szPykm, pField, (*it)->szHier,(*it)->szThname );

		if( (pos=strHier.find("'"))!=string::npos )
		{
			sprintf( szSql, "select 中文刊名, 年, 期, 年期,专题代码, 子栏目代码,专题子栏目代码,子栏目名称 from  cjfdtotal where 拼音刊名='%s?' and %s=\"%s?\" and thname='%s?'", (*it)->szPykm, pField, (*it)->szHier,(*it)->szThname );
		}

		if( (pos=strHier.find("\""))!=string::npos )
		{
			sprintf( szSql, "select 中文刊名, 年, 期, 年期,专题代码, 子栏目代码,专题子栏目代码,子栏目名称 from  cjfdtotal where 拼音刊名='%s?' and %s='%s?' and thname='%s?'", (*it)->szPykm, pField, (*it)->szHier,(*it)->szThname );
		}

		TPI_HRECORDSET hSet=NULL;
		m_ErrCode = IsConnected();
		for (int i=1;i<5;i++)
		{
			hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
			{
				CUtility::Logger()->d("超时 sql info.[在Exec sql %s]",szSql);
				Sleep( 1000 );
				hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
			}
			if (m_ErrCode != -6032)
			{
				break;
			}
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);
			Sleep(500);
		}
		if( !hSet || m_ErrCode < 0 )
		{
			char szMsg[MAX_SQL];
			sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
			CUtility::Logger()->d("error info.[%s]",szMsg);

			CloseRecFile( hFile );
			return m_ErrCode;
		}
		nRecordCount = TPI_GetRecordSetCount( hSet );
		if (nRecordCount == 0)
		{
			CUtility::Logger()->d("TYPE[%d]执行[%s]，结果集为0",nType,szSql);
		}
		if (nRecordCount > GETRECORDNUM*2)
		{
			m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
		}
		TPI_MoveFirst( hSet );

		//if( m_ErrCode < 0 )
		//{
		//	CloseRecFile( hFile );
		//	return m_ErrCode;
		//}

		if (taskData->nLog)
		{
			GetLocalTime(&systime);
			sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);
			CUtility::Logger()->d("write start time.[%s]",cTime);
		}
		long long nLen;
		for( int i = 0; i < nRecordCount; i ++ )
		{
			memset( item, 0, sizeof(ST_ITEMHIER) );
			nLen = (int)sizeof(item->szNameKm);
			TPI_GetFieldValue( hSet, 0, item->szNameKm, nLen );
			nLen = (int)sizeof(item->szYear);
			TPI_GetFieldValue( hSet, 1, item->szYear, nLen );
			nLen = (int)sizeof(item->szPeriod);
			TPI_GetFieldValue( hSet, 2, item->szPeriod, nLen );
			nLen = (int)sizeof(item->szNQ);
			TPI_GetFieldValue( hSet, 3, item->szNQ, nLen );
			nLen = (int)sizeof(item->szZtCode);
			TPI_GetFieldValue( hSet, 4, item->szZtCode, nLen );
			nLen = (int)sizeof(item->szColumnCode);
			TPI_GetFieldValue( hSet, 5, item->szColumnCode, nLen );
			nLen = (int)sizeof(item->szZtColumnCode);
			TPI_GetFieldValue( hSet, 6, item->szZtColumnCode, nLen );
			nLen = (int)sizeof(item->szName);
			TPI_GetFieldValue( hSet, 7, item->szName, nLen );


			WriteRecFile( hFile, "拼音刊名", (*it)->szPykm, false );
			WriteRecFile( hFile, "中文刊名", item->szNameKm, true );
			WriteRecFile( hFile, "期刊栏目层次",(*it)->szHier, true );
			WriteRecFile( hFile, "thname",(*it)->szThname, true );
			WriteRecFile( hFile, "年",item->szYear, true );
			WriteRecFile( hFile, "期",item->szPeriod, true );
			WriteRecFile( hFile, "年期",item->szNQ, true );
			WriteRecFile( hFile, "专题代码",item->szZtCode, true );
			WriteRecFile( hFile, "子栏目代码",item->szColumnCode, true );
			WriteRecFile( hFile, "专题子栏目代码",item->szZtColumnCode, true );
			WriteRecFile( hFile, "专题名称",item->szName, true );
			WriteRecFile( hFile, "更新日期", szDate, true );
			TPI_MoveNext(hSet);
		}
		TPI_CloseRecordSet(hSet);
		::fflush(hFile);
	}

	CloseRecFile( hFile );
	if (item)
	{
		delete item;
	}

	if( m_ErrCode >= 0 && nRecordCount > 0 )
	{
		nErrCode = nRecordCount;
		strcpy( pRetFile, szFileName );
	}
	if (taskData->nLog)
	{
		CUtility::Logger()->d("return result.nRecordCount[%d]",nRecordCount);
	}

	return nErrCode;

}

hfsbool		CKBConn::LoadPykm(vector<ST_PYKM*> &m_AllPykm)
{
	int m_ErrCode = IsConnected();
	long nRecordCount = 0;
	char szSql[128] = {0};
	sprintf( szSql, "select pykm,c_name from  %s", "cjfdbaseinfo" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );

	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	if (nRecordCount > GETRECORDNUM*2)
	{
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
	}
	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < nRecordCount; i ++ )
	{
		ST_PYKM *item = new ST_PYKM;
		memset( item, 0, sizeof(ST_PYKM) );

		nLen = (int)sizeof(item->szPykm);
		TPI_GetFieldValue( hSet, 0, item->szPykm, nLen );


		nLen = (int)sizeof(item->szName);
		TPI_GetFieldValue( hSet, 1, item->szName, nLen );

		m_AllPykm.push_back( item );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsbool		CKBConn::LoadPykmHier(vector<ST_PYKM*> &m_AllPykmHier)
{
	int m_ErrCode = IsConnected();
	long nRecordCount = 0;
	char szSql[128] = {0};
	sprintf( szSql, "select 拼音刊名,中文刊名,期刊栏目层次 from  %s", "kmcolu_hier" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	nRecordCount = TPI_GetRecordSetCount( hSet );

	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	if (nRecordCount > GETRECORDNUM*2)
	{
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
	}
	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < nRecordCount; i ++ )
	{
		ST_PYKM *item = new ST_PYKM;
		memset( item, 0, sizeof(ST_PYKM) );

		nLen = (int)sizeof(item->szPykm);
		TPI_GetFieldValue( hSet, 0, item->szPykm, nLen );


		nLen = (int)sizeof(item->szName);
		TPI_GetFieldValue( hSet, 1, item->szName, nLen );

		nLen = (int)sizeof(item->szHier);
		TPI_GetFieldValue( hSet, 2, item->szHier, nLen );

		m_AllPykmHier.push_back( item );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}

hfsbool		CKBConn::LoadPykmHierNq(vector<ST_PYKM*> &m_AllPykmHier)
{
	int m_ErrCode = IsConnected();
	long nRecordCount = 0;
	char szSql[128] = {0};
	sprintf( szSql, "select 拼音刊名,期刊栏目层次,thname from  %s", "kmcoluhier_nq" );
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[1024];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		if( m_ErrCode >= 0 )
			m_ErrCode = -1;

	    return m_ErrCode;
	}

	nRecordCount = TPI_GetRecordSetCount( hSet );

	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	if (nRecordCount > GETRECORDNUM*2)
	{
		m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*2);
	}
	long nLen;
	TPI_MoveFirst( hSet );
	for( int i = 0; i < nRecordCount; i ++ )
	{
		ST_PYKM *item = new ST_PYKM;
		memset( item, 0, sizeof(ST_PYKM) );

		nLen = (int)sizeof(item->szPykm);
		TPI_GetFieldValue( hSet, 0, item->szPykm, nLen );


		nLen = (int)sizeof(item->szHier);
		TPI_GetFieldValue( hSet, 1, item->szHier, nLen );

		nLen = (int)sizeof(item->szThname);
		TPI_GetFieldValue( hSet, 2, item->szThname, nLen );

		m_AllPykmHier.push_back( item );

		TPI_MoveNext( hSet );
	}
	TPI_CloseRecordSet(hSet);

	return true;
}


int CKBConn::RunTextGetTrue(DOWN_STATE * taskData)
{
	vector<string> items;
	int iCount= 0;
	int nCount= 1;

	vector<string>strDownFileList;

	CFolder f;
	vector<string>strFileList;
	vector<string>::iterator it2;
	f.GetFilesList( taskData->szLocPath, "*.zip", strFileList );
	CUtility::Logger()->d("zip文件所在目录szLocPath[%s].",taskData->szLocPath);
	string strZipFileName = "";
	string strZipFileDst = "";
	SetCurrentDirectory( taskData->szLocDirectory );
	CUtility::Logger()->d("unzip file path szLocDirectory[%s].",taskData->szLocDirectory);
	string strFilePathCurrent = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
		HZIP hz = OpenZip( (char *)it2->c_str(),0,ZIP_FILENAME );
		if (hz)
		{
			ZIPENTRY ze; 
			GetZipItem( hz, -1, &ze ); 
			int numitems = ze.index;
			for( int i = 0; i < numitems; i ++ )
			{ 
				GetZipItem( hz, i, &ze );
				SetCurrentDirectory( taskData->szLocDirectory );
				strFilePathCurrent = taskData->szLocDirectory;
				strFilePathCurrent += "\\";
				strFilePathCurrent += ze.name;
				_chmod( strFilePathCurrent.c_str(), _S_IWRITE );
				::DeleteFile( strFilePathCurrent.c_str() );
				if( UnzipItem( hz, i, ze.name, 0, ZIP_FILENAME ) == ZR_OK )
				{
					strDownFileList.push_back( ze.name );
					CUtility::Logger()->d("unzip file [%s].",ze.name);
					//_chmod( ze.name, _S_IWRITE );
					//DeleteFile( ze.name );
				}
				else
				{
					break;
				}
			}
			CloseZip( hz );
		}
		else
		{
			strZipFileDst = taskData->szLocDirectory;
			strZipFileDst += "\\";
			strZipFileDst += "nono\\";
	        strZipFileName = it2->c_str();
			int pos;
			if( (pos=strZipFileName.rfind("\\"))!=string::npos )
			{
				strZipFileName = strZipFileName.substr(pos+1,strZipFileName.length());
			}
			strZipFileDst += strZipFileName;

			if( !CopyFile(it2->c_str(), strZipFileDst.c_str(), FALSE) )
			{
				CUtility::Logger()->d("copy file fail from [%s] to [%s]",it2->c_str(),strZipFileDst.c_str());
			}
		}
	}

	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strDownFileList.size();
	CUtility::Logger()->d("解压缩文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strDownFileList.begin(); vit != strDownFileList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += *vit;
				::DeleteFile( strFileNew.c_str() );
				FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( taskData->szLocDirectory );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							sCurLine = szLine;

							pBegin = strchr( szLine, '\t' );
							if( !pBegin )
							{
								i = 0;
								memset( szLine, 0, sizeof(szLine) );
								bInsert = false;
								cChar=fgetc(fp);  
								continue;
							}

							pBegin ++;

							pEnd = strrchr( szLine, '\t' );
							if( !pEnd )
							{
								i = 0;
								memset( szLine, 0, sizeof(szLine) );
								bInsert = false;
								cChar=fgetc(fp);  
								continue;
							}
							*pEnd = '\0';

							memset(szFileName,0,sizeof(szFileName));
							strncpy( szFileName, pBegin, sizeof(szFileName) - 1 );
							_strupr( szFileName );
							sFileName = szFileName;
							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
							fwrite( szFileName, 1, (int)strlen(szFileName), hFileNew );
							fwrite( "\n", 1, 1, hFileNew );
							++nCount;
							if (nCount % 10000 == 0)
							{
								::fflush(hFileNew);
							}
						
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
				}  
				if (fp) fclose(fp);
				if (hFileNew) fclose(hFileNew);
				Sleep(500);
				strFilePath = taskData->szLocDirectory;
				strFilePath += "\\";
				strFilePath += *vit;
				_chmod( strFilePath.c_str(), _S_IWRITE );
				::DeleteFile( strFilePath.c_str() );
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("提取检索词列  end.");
		CUtility::Logger()->d("调用perl获取提取列的文件名 start.");
		string strCmdLine = "";
		//strCmdLine = "perl ";
		//strCmdLine += "1.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//system(strCmdLine.c_str());
		string strExe = "perl.exe";
		string strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\1.pl ";
		strParam += taskData->szLocDirectory;
		//strParam += " ";
		//strParam += taskData->szLocDirectory;
		//strParam += "\\文件名.txt";
		CUtility::Logger()->d("strParam[%s]",strParam.c_str());
		HANDLE		hUpdate			= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	

		CUtility::Logger()->d("调用perl获取提取列的文件名 end.");

		CUtility::Logger()->d("调用perl获取候选字列 start.");
		//strCmdLine = "perl ";
		//strCmdLine += "2-2.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//strCmdLine += "\\文件名.txt";
		//system(strCmdLine.c_str());
		strExe = "perl.exe";
		strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\2-2.pl ";
		strParam += taskData->szLocDirectory;
		strParam += "\\文件名.txt";
		//ShellExecute(NULL, _T("open"), strExe.c_str(), strParam.c_str(), NULL, SW_NORMAL);
		hUpdate	= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	
	
		CUtility::Logger()->d("调用perl获取候选字列 end.");
		int ipos = string::npos;
		for( vit = strDownFileList.begin(); vit != strDownFileList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());
				strFileName = *vit;
				if( (ipos=strFileName.rfind("."))!=string::npos )
				{
					strFileName = strFileName.substr(0,ipos);
				}
				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += strFileName.c_str();
				strFileNew += "_候选字串.txt";

				fp=fopen(strFileNew.c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					//try
					//{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 

							if( hashCptj.Lookup( szLine, nptrVal ) )
							{
								hashCptj.SetAt( szLine, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szLine, 1 );
							}	
							
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					//}
					//catch(exception ex)
					//{
					//	CUtility::Logger()->d("exception .");

					//}
				}  
				if (fp) fclose(fp);
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}

		CUtility::Logger()->d("写入词频文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有词频new.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		string strFileBak = taskData->szLocDirectory;
		strFileBak += "\\";
		strFileBak += "RESULT\\";
		strFileBak += "所有词频";
		strFileBak += taskData->szYearMonth;
		strFileBak += "before.txt";
		string strFileCur = taskData->szLocDirectory;
		strFileCur += "\\";
		strFileCur += "result\\";
		strFileCur += "所有词频.txt";
	
		if( !CopyFile(strFileCur.c_str(), strFileBak.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileCur.c_str(),strFileBak.c_str());
		}
		if( !CopyFile(strFileNew.c_str(), strFileCur.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileNew.c_str(),strFileCur.c_str());
		}
		CUtility::Logger()->d("写入词频文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrue2(DOWN_STATE * taskData)
{
	vector<string> items;
	int iCount= 0;
	int nCount= 1;

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "nono";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += *vit;
				::DeleteFile( strFileNew.c_str() );
				FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							sCurLine = szLine;

							pBegin = strchr( szLine, '\t' );
							if( !pBegin )
							{
								i = 0;
								memset( szLine, 0, sizeof(szLine) );
								bInsert = false;
								cChar=fgetc(fp);  
								continue;
							}

							pBegin ++;

							pEnd = strrchr( szLine, '\t' );
							if( !pEnd )
							{
								i = 0;
								memset( szLine, 0, sizeof(szLine) );
								bInsert = false;
								cChar=fgetc(fp);  
								continue;
							}
							*pEnd = '\0';

							memset(szFileName,0,sizeof(szFileName));
							strncpy( szFileName, pBegin, sizeof(szFileName) - 1 );
							_strupr( szFileName );
							sFileName = szFileName;
							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
							fwrite( szFileName, 1, (int)strlen(szFileName), hFileNew );
							fwrite( "\n", 1, 1, hFileNew );
							++nCount;
							if (nCount % 10000 == 0)
							{
								::fflush(hFileNew);
							}
						
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
				}  
				if (fp) fclose(fp);
				if (hFileNew) fclose(hFileNew);
				Sleep(500);
				strFilePath = taskData->szLocDirectory;
				strFilePath += "\\";
				strFilePath += *vit;
				_chmod( strFilePath.c_str(), _S_IWRITE );
				::DeleteFile( strFilePath.c_str() );
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("提取检索词列  end.");
		CUtility::Logger()->d("调用perl获取提取列的文件名 start.");
		string strCmdLine = "";
		//strCmdLine = "perl ";
		//strCmdLine += "1.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//system(strCmdLine.c_str());
		string strExe = "perl.exe";
		string strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\1.pl ";
		strParam += taskData->szLocDirectory;
		//strParam += " ";
		//strParam += taskData->szLocDirectory;
		//strParam += "\\文件名.txt";
		SetCurrentDirectory( taskData->szLocDirectory );
		CUtility::Logger()->d("strParam[%s]",strParam.c_str());
		HANDLE		hUpdate			= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	

		CUtility::Logger()->d("调用perl获取提取列的文件名 end.");

		CUtility::Logger()->d("调用perl获取候选字列 start.");
		//strCmdLine = "perl ";
		//strCmdLine += "2-2.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//strCmdLine += "\\文件名.txt";
		//system(strCmdLine.c_str());
		strExe = "perl.exe";
		strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\2-2.pl ";
		strParam += taskData->szLocDirectory;
		strParam += "\\文件名.txt";
		//ShellExecute(NULL, _T("open"), strExe.c_str(), strParam.c_str(), NULL, SW_NORMAL);
		hUpdate	= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	
	
		CUtility::Logger()->d("调用perl获取候选字列 end.");
		int ipos = string::npos;
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());
				strFileName = *vit;
				if( (ipos=strFileName.rfind("."))!=string::npos )
				{
					strFileName = strFileName.substr(0,ipos);
				}
				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += strFileName.c_str();
				strFileNew += "_候选字串.txt";

				fp=fopen(strFileNew.c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					//try
					//{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 

							if( hashCptj.Lookup( szLine, nptrVal ) )
							{
								hashCptj.SetAt( szLine, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szLine, 1 );
							}	
							
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					//}
					//catch(exception ex)
					//{
					//	CUtility::Logger()->d("exception .");

					//}
				}  
				if (fp) fclose(fp);
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}

		CUtility::Logger()->d("写入词频文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有词频new.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		string strFileBak = taskData->szLocDirectory;
		strFileBak += "\\";
		strFileBak += "RESULT\\";
		strFileBak += "所有词频";
		strFileBak += taskData->szYearMonth;
		strFileBak += "before.txt";
		string strFileCur = taskData->szLocDirectory;
		strFileCur += "\\";
		strFileCur += "result\\";
		strFileCur += "所有词频.txt";
	
		if( !CopyFile(strFileCur.c_str(), strFileBak.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileCur.c_str(),strFileBak.c_str());
		}
		if( !CopyFile(strFileNew.c_str(), strFileCur.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileNew.c_str(),strFileCur.c_str());
		}
		CUtility::Logger()->d("写入词频文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrue1(DOWN_STATE * taskData)
{
	vector<string> items;
	int iCount= 0;
	int nCount= 1;

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;

	f.GetFilesList( taskData->szLocPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",taskData->szLocPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("copy检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += *vit;
				::DeleteFile( strFileNew.c_str() );
				FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				strFilePath = taskData->szLocPath;
				strFilePath += "\\";
				strFilePath += *vit;

				if( !CopyFile(strFilePath.c_str(), strFileNew.c_str(), FALSE) )
				{
					CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFilePath.c_str(),strFileNew.c_str());
				}
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("copy检索词列  end.");
		CUtility::Logger()->d("调用perl获取提取列的文件名 start.");
		string strCmdLine = "";
		//strCmdLine = "perl ";
		//strCmdLine += "1.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//system(strCmdLine.c_str());
		string strExe = "perl.exe";
		string strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\1.pl ";
		strParam += taskData->szLocDirectory;
		//strParam += " ";
		//strParam += taskData->szLocDirectory;
		//strParam += "\\文件名.txt";
		SetCurrentDirectory( taskData->szLocDirectory );
		CUtility::Logger()->d("strParam[%s]",strParam.c_str());
		HANDLE		hUpdate			= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	

		CUtility::Logger()->d("调用perl获取提取列的文件名 end.");

		CUtility::Logger()->d("调用perl获取候选字列 start.");
		//strCmdLine = "perl ";
		//strCmdLine += "2-2.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//strCmdLine += "\\文件名.txt";
		//system(strCmdLine.c_str());
		strExe = "perl.exe";
		strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\2-2.pl ";
		strParam += taskData->szLocDirectory;
		strParam += "\\文件名.txt";
		//ShellExecute(NULL, _T("open"), strExe.c_str(), strParam.c_str(), NULL, SW_NORMAL);
		hUpdate	= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	
	
		CUtility::Logger()->d("调用perl获取候选字列 end.");
		int ipos = string::npos;
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());
				strFileName = *vit;
				if( (ipos=strFileName.rfind("."))!=string::npos )
				{
					strFileName = strFileName.substr(0,ipos);
				}
				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				strFileNew += "search_";
				strFileNew += strFileName.c_str();
				strFileNew += "_候选字串.txt";

				fp=fopen(strFileNew.c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					//try
					//{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 

							if( hashCptj.Lookup( szLine, nptrVal ) )
							{
								hashCptj.SetAt( szLine, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szLine, 1 );
							}	
							
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					//}
					//catch(exception ex)
					//{
					//	CUtility::Logger()->d("exception .");

					//}
				}  
				if (fp) fclose(fp);
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}

		CUtility::Logger()->d("写入词频文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有词频new.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		string strFileBak = taskData->szLocDirectory;
		strFileBak += "\\";
		strFileBak += "RESULT\\";
		strFileBak += "所有词频";
		strFileBak += taskData->szYearMonth;
		strFileBak += "before.txt";
		string strFileCur = taskData->szLocDirectory;
		strFileCur += "\\";
		strFileCur += "result\\";
		strFileCur += "所有词频.txt";
	
		if( !CopyFile(strFileCur.c_str(), strFileBak.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileCur.c_str(),strFileBak.c_str());
		}
		if( !CopyFile(strFileNew.c_str(), strFileCur.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileNew.c_str(),strFileCur.c_str());
		}
		CUtility::Logger()->d("写入词频文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrue3(DOWN_STATE * taskData)
{
	vector<string> items;
	int iCount= 0;
	int nCount= 1;

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;

	f.GetFilesList( taskData->szLocPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",taskData->szLocPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("copy检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				//strFileNew += "search_";
				strFileNew += *vit;
				::DeleteFile( strFileNew.c_str() );
				FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				strFilePath = taskData->szLocPath;
				strFilePath += "\\";
				strFilePath += *vit;

				if( !CopyFile(strFilePath.c_str(), strFileNew.c_str(), FALSE) )
				{
					CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFilePath.c_str(),strFileNew.c_str());
				}
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("copy检索词列  end.");
		CUtility::Logger()->d("调用perl获取提取列的文件名 start.");
		string strCmdLine = "";
		//strCmdLine = "perl ";
		//strCmdLine += "1.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//system(strCmdLine.c_str());
		string strExe = "perl.exe";
		string strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\1.pl ";
		strParam += taskData->szLocDirectory;
		//strParam += " ";
		//strParam += taskData->szLocDirectory;
		//strParam += "\\文件名.txt";
		SetCurrentDirectory( taskData->szLocDirectory );
		CUtility::Logger()->d("strParam[%s]",strParam.c_str());
		HANDLE		hUpdate			= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	

		CUtility::Logger()->d("调用perl获取提取列的文件名 end.");

		CUtility::Logger()->d("调用perl获取候选字列 start.");
		//strCmdLine = "perl ";
		//strCmdLine += "2-2.pl ";
		//strCmdLine += taskData->szLocDirectory;
		//strCmdLine += "\\文件名.txt";
		//system(strCmdLine.c_str());
		strExe = "perl.exe";
		strParam = "";
		strParam += taskData->szLocDirectory;
		strParam += "\\2-3.pl ";
		strParam += taskData->szLocDirectory;
		strParam += "\\文件名.txt";
		//ShellExecute(NULL, _T("open"), strExe.c_str(), strParam.c_str(), NULL, SW_NORMAL);
		hUpdate	= NULL;
		if (!ExecuteProcess(strExe.c_str(), strParam.c_str(), hUpdate))
		{					
			CUtility::Logger()->d("ExecuteProcess create fail");	
		}
		WaitForSingleObject(hUpdate, INFINITE);	
	
		CUtility::Logger()->d("调用perl获取候选字列 end.");
		int ipos = string::npos;
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());
				strFileName = *vit;
				if( (ipos=strFileName.rfind("."))!=string::npos )
				{
					strFileName = strFileName.substr(0,ipos);
				}
				strFileNew = taskData->szLocDirectory;
				strFileNew += "\\";
				//strFileNew += "search_";
				strFileNew += strFileName.c_str();
				strFileNew += "_候选字串.txt";

				fp=fopen(strFileNew.c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					//try
					//{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 

							if( hashCptj.Lookup( szLine, nptrVal ) )
							{
								hashCptj.SetAt( szLine, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szLine, 1 );
							}	
							
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
						cChar=fgetc(fp);  
					//}
					//catch(exception ex)
					//{
					//	CUtility::Logger()->d("exception .");

					//}
				}  
				if (fp) fclose(fp);
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}

		CUtility::Logger()->d("写入词频文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有词频new.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		string strFileBak = taskData->szLocDirectory;
		strFileBak += "\\";
		strFileBak += "RESULT\\";
		strFileBak += "所有词频";
		strFileBak += taskData->szYearMonth;
		strFileBak += "before2.txt";
		string strFileCur = taskData->szLocDirectory;
		strFileCur += "\\";
		strFileCur += "result\\";
		strFileCur += "所有词频.txt";
	
		if( !CopyFile(strFileCur.c_str(), strFileBak.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileCur.c_str(),strFileBak.c_str());
		}
		if( !CopyFile(strFileNew.c_str(), strFileCur.c_str(), FALSE) )
		{
			CUtility::Logger()->d("copy file fail from [%s] to [%s]",strFileNew.c_str(),strFileCur.c_str());
		}
		CUtility::Logger()->d("写入词频文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

hfsbool		CKBConn::LoadCptj(DOWN_STATE * taskData,HS_MapStrToInt &m_Cptj)
{
	string strFileNew = taskData->szLocDirectory;
	strFileNew += "\\";
	strFileNew += "result\\";
	strFileNew += "所有词频.txt";
	FILE   *fp=NULL;  
	fp=fopen(strFileNew.c_str(),"r");  
	if (!fp)
	{
		return false;
	}
	char szLine[4096] = {0};
	char szKey[32] = {0};
	char szValue[32] = {0};
    char   cChar;  
	string strLine = "";
	bool bInsert = false;
	vector<string> items;
	int iCount = 0;
	int nCount = 1;
	int nptrVal = 0;
	int nLen = 4096;
	char *pBegin, *pEnd;
	int i=0;  
	string sPreLine = "";
	string sCurLine = "";
	string sFileName = "";
	string::size_type   pos(0);     
	cChar=fgetc(fp);  
	while(!feof(fp))  
	{  
		//try
		//{
			if (cChar=='\r')
			{
				cChar=fgetc(fp);  
				continue;
			}
			if (cChar!='\n')
			{
				szLine[i]=cChar;  
			}
			else
			{
				szLine[i]='\0'; 
				sCurLine = szLine;
				items.clear();
				CUtility::Str()->CStrToStrings(szLine,items,'\t');
				iCount = items.size();
				if (iCount == 2)
				{
					strcpy_s(szKey,items[0].c_str());
					strcpy_s(szValue,items[1].c_str());
					nptrVal = _atoi64(szValue);
					if( m_Cptj.Lookup( szKey, nptrVal ) )
					{
						m_Cptj.SetAt( szKey, nptrVal + 1 );
					}
					else
					{
						m_Cptj.SetAt( szKey, nptrVal );
					}	
				}

				sPreLine = szLine;
				bInsert = true;
			}
			if (!bInsert)
			{
				++i;  
			}
			else
			{
				i = 0;
				memset( szLine, 0, sizeof(szLine) );
				bInsert = false;
			}
			cChar=fgetc(fp);  
		//}
		//catch(exception ex)
		//{
		//	CUtility::Logger()->d("exception .");

		//}
	}  
	if (fp) fclose(fp);


	return true;
}

BOOL 		CKBConn::ExecuteProcess(LPCTSTR cmd, LPCTSTR param, HANDLE& hProcess)
{
	PROCESS_INFORMATION pi;
	STARTUPINFO			si;
	TCHAR				szCmdLine[MAX_PATH];
	memset(szCmdLine, 0, sizeof(szCmdLine));
	
	ZeroMemory(&si,sizeof(si));
	si.cb = sizeof(si);
	
	lstrcpy(szCmdLine, _T("\""));
	lstrcat(szCmdLine, cmd);
	if(0 != _tcslen(param))
	{
		lstrcat(szCmdLine, _T("\" "));													
		lstrcat(szCmdLine, param);
	}

	BOOL bRetCode = CreateProcess(NULL,szCmdLine,NULL,NULL,FALSE,NORMAL_PRIORITY_CLASS,NULL,NULL,&si,&pi);

	if (FALSE == bRetCode) 
	{
		CUtility::Logger()->d("CreateProcess Error[%s]", szCmdLine, 0);
		return FALSE;
	}
	hProcess = pi.hProcess;

	return TRUE;
}

int CKBConn::RunTextGetTrue4(DOWN_STATE * taskData)
{
	int nRet = 0;
	int nCount = 0;
	char szSql[1024];
	time_t  t;
	struct tm *newtime;	
	time(&t);
	newtime = localtime( &t );
	char szDate[20];
	sprintf( szDate, "%#04d-%#02d-%#02d",
			 newtime->tm_year + 1900, newtime->tm_mon + 1, newtime->tm_mday );

	char szFile[MAX_PATH];
	char szFile01[MAX_PATH];
	char szFile02[MAX_PATH];
	char szFile03[MAX_PATH];
	char szFileResult[MAX_PATH];

	GetModuleFileName( NULL, szFile, sizeof(szFile) );
	strcpy(szFile01,szFile); 
	strcpy(szFile02,szFile); 
	strcpy(szFile03,szFile); 
	strcpy(szFileResult,szFile); 
	strcpy( strrchr(szFile, '\\') + 1, taskData->szTableName );
	strcat( szFile, szDate );
	strcat( szFile, ".txt" );
	strcpy( strrchr(szFile01, '\\') + 1, taskData->szTableName );
	strcat( szFile01, szDate );
	strcat( szFile01, "_01" );
	strcat( szFile01, ".txt" );
	strcpy( strrchr(szFile02, '\\') + 1, taskData->szTableName );
	strcat( szFile02, szDate );
	strcat( szFile02, "_02" );
	strcat( szFile02, ".txt" );
	strcpy( strrchr(szFile03, '\\') + 1, taskData->szTableName );
	strcat( szFile03, szDate );
	strcat( szFile03, "_03" );
	strcat( szFile03, ".txt" );

	::DeleteFile( szFile );
	::DeleteFile( szFile01 );
	::DeleteFile( szFile02 );
	::DeleteFile( szFile03 );

	sprintf( szSql, "select %s from %s into '%s'", taskData->szFieldName, taskData->szTableName, szFile);
	nRet = ExecMgrSql( szSql );
	if( nRet < 0 )
	{
		CUtility::Logger()->d("exec [%s] result[%d]", szSql, nRet);
		return nRet;
	}
	Sleep(500);

	HS_MapStrToInt hashCptj;
	hashCptj.AllocMemoryPool();
	hashCptj.InitHashTable(4096);

	LoadCpFromFile(szFile, hashCptj);
	LoadCpFromFile(szFile01, hashCptj);
	LoadCpFromFile(szFile02, hashCptj);
	LoadCpFromFile(szFile03, hashCptj);
	CUtility::Logger()->d("写入词频文件开始.");

	strcpy( strrchr(szFileResult, '\\') + 1, taskData->szTableName );
	strcat( szFileResult, taskData->szFieldName );
	strcat( szFileResult, "词频.txt" );
	::DeleteFile( szFileResult );
	FILE *hFileNew = OpenRecFile( szFileResult );

	HITEM_POSITION pos;
	char *key;
	int nptrVal = 0;
	char szValue[32]={0};
	for( pos = hashCptj.GetStartPosition(); pos != NULL; )
	{
		hashCptj.GetNextAssoc( pos, key, nptrVal );
		itoa(nptrVal,szValue,10);
		fwrite( key, 1, (int)strlen(key), hFileNew );
		fwrite( "\t", 1, 1, hFileNew );
		fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
		fwrite( "\n", 1, 1, hFileNew );
	}
	if (hFileNew) fclose(hFileNew);

	CUtility::Logger()->d("写入词频文件结束.");

	return 0;
}

hfsbool	CKBConn::LoadCpFromFile(const char * szFileName, HS_MapStrToInt &hashCptj)
{
	FILE *hFileRec = fopen(szFileName,"r"); 
	if (!hFileRec)
	{
		CUtility::Logger()->d("file [%s] not exist", szFileName);
		return 0;
	}
	int nCount = 0;
	int nptrVal;
	string strLine = "";
	char line[255] = {0};
	char szKey[255] = {0};
	vector<string> items;
	int nSize = 0;
	int nPos = string::npos;
	int i = 0;
	bool bInsert = false;
	char cChar=0;
	cChar=fgetc(hFileRec);  
	while(!feof(hFileRec))  
    {  
		if (cChar=='\r')
		{
	        cChar=fgetc(hFileRec);  
			continue;
		}
		if (cChar!='\n')
		{
			line[i]=cChar;  
		}
		else
		{
		    line[i]='\0'; 
			strLine = line;
			if (strLine != "<REC>")
			{
				items.clear();
				CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
				nSize = items.size();
				if (nSize == 2)
				{
					strLine = items[1];
					if( (nPos=strLine.find(";"))!=string::npos )
					{
						items.clear();
						CUtility::Str()->CStrToStrings(strLine.c_str(), items, ';');
						nSize = items.size();
						for (int j = 0; j < nSize; ++j)
						{
							memset(szKey,0,sizeof(szKey));
							strcpy(szKey,items[j].c_str());
							if( hashCptj.Lookup( szKey, nptrVal ) )
							{
								hashCptj.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szKey, 1 );
							}		
						}
					}
					else if ( (nPos=strLine.find(","))!=string::npos )
					{
						items.clear();
						CUtility::Str()->CStrToStrings(strLine.c_str(), items, ',');
						nSize = items.size();
						for (int j = 0; j < nSize; ++j)
						{
							memset(szKey,0,sizeof(szKey));
							strcpy(szKey,items[j].c_str());
							if( hashCptj.Lookup( szKey, nptrVal ) )
							{
								hashCptj.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szKey, 1 );
							}		
						}
					}
					else
					{
							memset(szKey,0,sizeof(szKey));
							strcpy(szKey,strLine.c_str());
							if( hashCptj.Lookup( szKey, nptrVal ) )
							{
								hashCptj.SetAt( szKey, nptrVal + 1 );
							}
							else
							{
								hashCptj.SetAt( szKey, 1 );
							}		
					}
				}
			}
		
			bInsert = true;
			++nCount;
			if (nCount%100000 == 0)
			{
				CUtility::Logger()->d("read rec line[%d]",nCount);
			}
		}
		if (!bInsert)
		{
			++i;  
		}
		else
		{
			i = 0;
			bInsert = false;
		}
		cChar=fgetc(hFileRec);  
    }  
	if(hFileRec)
	{
		fclose(hFileRec);
	}
	return true;
}

int CKBConn::RunTextGetTrueTm(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "201507";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				//strFileNew = taskData->szLocDirectory;
				//strFileNew += "\\";
				//strFileNew += "search_";
				//strFileNew += *vit;
				//::DeleteFile( strFileNew.c_str() );
				//FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							if (strLine != "<REC>")
							{
								items.clear();
								CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
								if (items.size()==2)
								{
									if ( items[1]!="")
									{
										if (items[0]=="<条目名称>")
										{
											if (items[1].length() < 256)
											{
												strTmName = items[1];
											}
											else
											{
												strTmName = items[1].substr(0,255);
											}
										}
										if (items[0]=="<专题代码>")
										{
											strZtCode = items[1];
										}
										if (items[0]=="<是否有释文>")
										{
											strSf = items[1];
										}
									}
								}
							}
							else
							{
								if (strSf == "Y")
								{
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strncpy( szKey, items[j].c_str(), 4 );
										strcat( szKey, "#-#" );
										strcat( szKey, strTmName.c_str() );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}
					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}
								}
							}


							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
					
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  
				if (strSf == "Y")
				{
					memset(szFileName,0,sizeof(szFileName));
					strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
					_strupr( szFileName );
					sFileName = szFileName;
					items.clear();
					CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
					for (int j=0;j<items.size();j++)
					{
						memset(szKey,0,sizeof(szKey));
						strncpy( szKey, items[j].c_str(), 4 );
						strcat( szKey, "#-#" );
						strcat( szKey, strTmName.c_str() );
						if( hashCptj.Lookup( szKey, nptrVal ) )
						{
							hashCptj.SetAt( szKey, nptrVal + 1 );
						}
						else
						{
							hashCptj.SetAt( szKey, 1 );
						}		
					}

				if (fp) fclose(fp);
				//Sleep(500);
				//strFilePath = taskData->szLocDirectory;
				//strFilePath += "\\";
				//strFilePath += *vit;
				//_chmod( strFilePath.c_str(), _S_IWRITE );
				//::DeleteFile( strFilePath.c_str() );
			}
		}
		catch(exception e)
		{
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有条目new.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm5(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;

	map<hfsstring,hfsstring> m_ZJCls;
	LoadZJCLSMap(m_ZJCls);

	map<hfsstring,hfsstring>::iterator itm;
	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	string strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有条目new.txt";
		strFileList.push_back(strFileNew);

	strTxtPath += "\\";
	strTxtPath += "RESULT";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	////f.GetFilesList( szTxtPath, "*.txt", strFileList );
	//CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	strFileNew = "";
	char szFileName[1024]={0};
	//test
	//strDownFileList.clear();
	//strDownFileList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;
							strZtCode = strLine.substr(0,4);
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									itm = m_ZJCls.find(szFileName);
									if (itm != m_ZJCls.end())
									{
										strcat(szFileName,"-");
										strcat(szFileName,itm->second.c_str());
										if( hashCptj.Lookup( szFileName, nptrVal ) )
										{
											hashCptj.SetAt( szFileName, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szFileName, 1 );
										}	
									}
									else
									{
										strcat(szFileName,"-");
										strcat(szFileName,"nomatch");
										if( hashCptj.Lookup( szFileName, nptrVal ) )
										{
											hashCptj.SetAt( szFileName, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szFileName, 1 );
										}	
									}
														
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  

				if (fp) fclose(fp);
		}
		catch(exception e)
		{
			if (fp) fclose(fp);
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入条目code文件开始.");
		strFileNew = taskData->szLocDirectory;
		strFileNew += "\\";
		strFileNew += "RESULT\\";
		strFileNew += "所有条目code.txt";
		::DeleteFile( strFileNew.c_str() );
		FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

		HITEM_POSITION pos;
		char *key;
		char szValue[32]={0};
		for( pos = hashCptj.GetStartPosition(); pos != NULL; )
		{
			hashCptj.GetNextAssoc( pos, key, nptrVal );
			itoa(nptrVal,szValue,10);
			fwrite( key, 1, (int)strlen(key), hFileNew );
			fwrite( "\t", 1, 1, hFileNew );
			fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
			fwrite( "\n", 1, 1, hFileNew );
		}
		if (hFileNew) fclose(hFileNew);
		CUtility::Logger()->d("写入条目code文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm7(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "201507";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath.c_str());
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strFileNameList.clear();
	//strFileNameList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				//strFileNew = taskData->szLocDirectory;
				//strFileNew += "\\";
				//strFileNew += "search_";
				//strFileNew += *vit;
				//::DeleteFile( strFileNew.c_str() );
				//FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							if (strLine != "<REC>")
							{
								items.clear();
								CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
								if (items.size()==2)
								{
									if ( items[1]!="")
									{
										if (items[0]=="<条目名称>")
										{
											if (items[1].length() < 256)
											{
												strTmName = items[1];
											}
											else
											{
												strTmName = items[1].substr(0,255);
											}
										}
										if (items[0]=="<专题代码>")
										{
											strZtCode = items[1];
										}
										if (items[0]=="<是否有释文>")
										{
											strSf = items[1];
										}
									}
								}
							}
							else
							{
								if (strSf == "Y")
								{
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strncpy( szKey, items[j].c_str(), 1 );
										strcat( szKey, "#-#" );
										strcat( szKey, strTmName.c_str() );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}
					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}
								}
							}


							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
					
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  
				if (strSf == "Y")
				{
					memset(szFileName,0,sizeof(szFileName));
					strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
					_strupr( szFileName );
					sFileName = szFileName;
					items.clear();
					CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
					for (int j=0;j<items.size();j++)
					{
						memset(szKey,0,sizeof(szKey));
						strncpy( szKey, items[j].c_str(), 1 );
						strcat( szKey, "#-#" );
						strcat( szKey, strTmName.c_str() );
						if( hashCptj.Lookup( szKey, nptrVal ) )
						{
							hashCptj.SetAt( szKey, nptrVal + 1 );
						}
						else
						{
							hashCptj.SetAt( szKey, 1 );
						}		
					}

				if (fp) fclose(fp);
			}
		}
		catch(exception e)
		{
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入文件开始.");

		string strChar = "";
		map<string,int> mapValueSingle;
		map<string,int>::iterator mapit;
		multimap<int,string> mapValue;
		multimap<int,string>::reverse_iterator mapvalueit;
		for (int i=0;i<10;++i)
		{
			mapValue.clear();
			mapValueSingle.clear();
			strFileNew = taskData->szLocDirectory;
			strFileNew += "\\";
			strFileNew += "RESULT\\";
			strFileNew += gZj[i];
			strFileNew += "专辑全部排重词目.txt";
			::DeleteFile( strFileNew.c_str() );
			FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

			HITEM_POSITION pos;
			char *key;
			char szValue[32]={0};
			for( pos = hashCptj.GetStartPosition(); pos != NULL; )
			{
				hashCptj.GetNextAssoc( pos, key, nptrVal );
				strChar = key;
				if (strChar.substr(0,1) == gZj[i])
				{
					mapValueSingle.insert(make_pair(key,nptrVal));
				}
			}
			for(mapit = mapValueSingle.begin(); mapit != mapValueSingle.end(); ++mapit)
			{
				mapValue.insert(make_pair(mapit->second,mapit->first));
			}
			for(mapvalueit = mapValue.rbegin(); mapvalueit != mapValue.rend(); ++mapvalueit)
			{
				itoa(mapvalueit->first,szValue,10);
				fwrite( mapvalueit->second.c_str(), 1, (int)strlen(mapvalueit->second.c_str()), hFileNew );
				fwrite( "\t", 1, 1, hFileNew );
				fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
				fwrite( "\n", 1, 1, hFileNew );
			}
			if (hFileNew) fclose(hFileNew);
		}
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm8(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "201507";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath.c_str());
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strFileNameList.clear();
	//strFileNameList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				//strFileNew = taskData->szLocDirectory;
				//strFileNew += "\\";
				//strFileNew += "search_";
				//strFileNew += *vit;
				//::DeleteFile( strFileNew.c_str() );
				//FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string strType = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							if (strLine != "<REC>")
							{
								items.clear();
								CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
								if (items.size()==2)
								{
									if ( items[1]!="")
									{
										if (items[0]=="<条目名称>")
										{
											if (items[1].length() < 256)
											{
												strTmName = items[1];
											}
											else
											{
												strTmName = items[1].substr(0,255);
											}
										}
										if (items[0]=="<专题代码>")
										{
											strZtCode = items[1];
										}
										if (items[0]=="<是否有释文>")
										{
											strSf = items[1];
										}
										if (items[0]=="<工具书类型名称>")
										{
											strType = items[1];
										}
									}
								}
							}
							else
							{
								if ((strSf == "Y") && ((strType == gCs1[0]) || (strType==gCs1[1]) || (strType==gCs1[2]) || (strType==gCs1[3]) || (strType==gCs1[4])))
								{
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strncpy( szKey, items[j].c_str(), 1 );
										strcat( szKey, "#-#" );
										strcat( szKey, strTmName.c_str() );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}
					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}
								}
							}


							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
					
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  
				if ((strSf == "Y") && ((strType == gCs1[0]) || (strType==gCs1[1]) || (strType==gCs1[2]) || (strType==gCs1[3]) || (strType==gCs1[4])))
				{
					memset(szFileName,0,sizeof(szFileName));
					strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
					_strupr( szFileName );
					sFileName = szFileName;
					items.clear();
					CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
					for (int j=0;j<items.size();j++)
					{
						memset(szKey,0,sizeof(szKey));
						strncpy( szKey, items[j].c_str(), 1 );
						strcat( szKey, "#-#" );
						strcat( szKey, strTmName.c_str() );
						if( hashCptj.Lookup( szKey, nptrVal ) )
						{
							hashCptj.SetAt( szKey, nptrVal + 1 );
						}
						else
						{
							hashCptj.SetAt( szKey, 1 );
						}		
					}

				if (fp) fclose(fp);
			}
		}
		catch(exception e)
		{
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入文件开始.");

		string strChar = "";
		map<string,int> mapValueSingle;
		map<string,int>::iterator mapit;
		multimap<int,string> mapValue;
		multimap<int,string>::reverse_iterator mapvalueit;
		for (int i=0;i<10;++i)
		{
			mapValue.clear();
			mapValueSingle.clear();
			strFileNew = taskData->szLocDirectory;
			strFileNew += "\\";
			strFileNew += "RESULT\\";
			strFileNew += gZj[i];
			strFileNew += "专辑全部辞书排重词目.txt";
			::DeleteFile( strFileNew.c_str() );
			FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

			HITEM_POSITION pos;
			char *key;
			char szValue[32]={0};
			for( pos = hashCptj.GetStartPosition(); pos != NULL; )
			{
				hashCptj.GetNextAssoc( pos, key, nptrVal );
				strChar = key;
				if (strChar.substr(0,1) == gZj[i])
				{
					mapValueSingle.insert(make_pair(key,nptrVal));
				}
			}
			for(mapit = mapValueSingle.begin(); mapit != mapValueSingle.end(); ++mapit)
			{
				mapValue.insert(make_pair(mapit->second,mapit->first));
			}
			for(mapvalueit = mapValue.rbegin(); mapvalueit != mapValue.rend(); ++mapvalueit)
			{
				itoa(mapvalueit->first,szValue,10);
				fwrite( mapvalueit->second.c_str(), 1, (int)strlen(mapvalueit->second.c_str()), hFileNew );
				fwrite( "\t", 1, 1, hFileNew );
				fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
				fwrite( "\n", 1, 1, hFileNew );
			}
			if (hFileNew) fclose(hFileNew);
		}
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm9(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "201507";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath.c_str());
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strFileNameList.clear();
	//strFileNameList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				//strFileNew = taskData->szLocDirectory;
				//strFileNew += "\\";
				//strFileNew += "search_";
				//strFileNew += *vit;
				//::DeleteFile( strFileNew.c_str() );
				//FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string strType = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							if (strLine != "<REC>")
							{
								items.clear();
								CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
								if (items.size()==2)
								{
									if ( items[1]!="")
									{
										if (items[0]=="<条目名称>")
										{
											if (items[1].length() < 256)
											{
												strTmName = items[1];
											}
											else
											{
												strTmName = items[1].substr(0,255);
											}
										}
										if (items[0]=="<专题代码>")
										{
											strZtCode = items[1];
										}
										if (items[0]=="<是否有释文>")
										{
											strSf = items[1];
										}
										if (items[0]=="<工具书类型名称>")
										{
											strType = items[1];
										}
									}
								}
							}
							else
							{
								if ((strSf == "Y") && ((strType == gCs2[0]) || (strType==gCs2[1]) || (strType==gCs2[2]) || (strType==gCs2[3])))
								{
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strncpy( szKey, items[j].c_str(), 1 );
										strcat( szKey, "#-#" );
										strcat( szKey, strTmName.c_str() );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}
					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}
								}
							}


							//if(strstr(sFileName.c_str()," ")){
							//	Trim(szFileName,' ');
							//}
					
							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  
				if ((strSf == "Y") && ((strType == gCs2[0]) || (strType==gCs2[1]) || (strType==gCs2[2]) || (strType==gCs2[3])))
				{
					memset(szFileName,0,sizeof(szFileName));
					strncpy( szFileName, strZtCode.c_str(), sizeof(szFileName) - 1 );
					_strupr( szFileName );
					sFileName = szFileName;
					items.clear();
					CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
					for (int j=0;j<items.size();j++)
					{
						memset(szKey,0,sizeof(szKey));
						strncpy( szKey, items[j].c_str(), 1 );
						strcat( szKey, "#-#" );
						strcat( szKey, strTmName.c_str() );
						if( hashCptj.Lookup( szKey, nptrVal ) )
						{
							hashCptj.SetAt( szKey, nptrVal + 1 );
						}
						else
						{
							hashCptj.SetAt( szKey, 1 );
						}		
					}

				if (fp) fclose(fp);
			}
		}
		catch(exception e)
		{
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入文件开始.");

		string strChar = "";
		map<string,int> mapValueSingle;
		map<string,int>::iterator mapit;
		multimap<int,string> mapValue;
		multimap<int,string>::reverse_iterator mapvalueit;
		for (int i=0;i<10;++i)
		{
			mapValue.clear();
			mapValueSingle.clear();
			strFileNew = taskData->szLocDirectory;
			strFileNew += "\\";
			strFileNew += "RESULT\\";
			strFileNew += gZj[i];
			strFileNew += "专辑汉英辞书排重词目.txt";
			::DeleteFile( strFileNew.c_str() );
			FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

			HITEM_POSITION pos;
			char *key;
			char szValue[32]={0};
			for( pos = hashCptj.GetStartPosition(); pos != NULL; )
			{
				hashCptj.GetNextAssoc( pos, key, nptrVal );
				strChar = key;
				if (strChar.substr(0,1) == gZj[i])
				{
					mapValueSingle.insert(make_pair(key,nptrVal));
				}
			}
			for(mapit = mapValueSingle.begin(); mapit != mapValueSingle.end(); ++mapit)
			{
				mapValue.insert(make_pair(mapit->second,mapit->first));
			}
			for(mapvalueit = mapValue.rbegin(); mapvalueit != mapValue.rend(); ++mapvalueit)
			{
				itoa(mapvalueit->first,szValue,10);
				fwrite( mapvalueit->second.c_str(), 1, (int)strlen(mapvalueit->second.c_str()), hFileNew );
				fwrite( "\t", 1, 1, hFileNew );
				fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
				fwrite( "\n", 1, 1, hFileNew );
			}
			if (hFileNew) fclose(hFileNew);
		}
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm10(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "result";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath.c_str());
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	int nCurVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strFileNameList.clear();
	//strFileNameList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				if (!CheckDo(vit->c_str()))
				{
					continue;
				}
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string strType = "";
				string::size_type   pos(0);     
				memset(szKey,0,sizeof(szKey));
				strncpy( szKey, vit->c_str(), 1 );
				strcat( szKey, "专辑总条目数" );
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							items.clear();
							CUtility::Str()->CStrToStrings(strLine.c_str(), items, '\t');
							if (items.size()==2)
							{
								if ( items[1]!="")
								{
									nCurVal = _atoi64(items[1].c_str());
									if( hashCptj.Lookup( szKey, nptrVal ) )
									{
										hashCptj.SetAt( szKey, nptrVal + nCurVal );
									}
									else
									{
										hashCptj.SetAt( szKey, nCurVal );
									}		
								}
							}
							++nCount;
							if (nCount%100000 == 0)
							{
								CUtility::Logger()->d("read rec line[%d]",nCount);
							}

							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  

				if (fp) fclose(fp);
			}
			catch(exception e)
			{
				CUtility::Logger()->d("异常发生.");
			}
		}
		CUtility::Logger()->d("写入文件开始.");

		string strChar = "";
		map<string,int> mapValueSingle;
		map<string,int>::iterator mapit;
		multimap<int,string> mapValue;
		multimap<int,string>::reverse_iterator mapvalueit;
			mapValue.clear();
			mapValueSingle.clear();
			strFileNew = taskData->szLocDirectory;
			strFileNew += "\\";
			strFileNew += "RESULT\\";
			strFileNew += "专辑总条目数.txt";
			::DeleteFile( strFileNew.c_str() );
			FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

			HITEM_POSITION pos;
			char *key;
			char szValue[32]={0};
			for( pos = hashCptj.GetStartPosition(); pos != NULL; )
			{
				hashCptj.GetNextAssoc( pos, key, nptrVal );
				mapValueSingle.insert(make_pair(key,nptrVal));
			}
			for(mapit = mapValueSingle.begin(); mapit != mapValueSingle.end(); ++mapit)
			{
				itoa(mapit->second,szValue,10);
				fwrite( mapit->first.c_str(), 1, (int)strlen(mapit->first.c_str()), hFileNew );
				fwrite( ",", 1, 1, hFileNew );
				fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
				fwrite( "\n", 1, 1, hFileNew );
			}
			if (hFileNew) fclose(hFileNew);
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

int CKBConn::RunTextGetTrueTm11(DOWN_STATE * taskData)
{
	vector<string> items;
	long iCount= 0;
	long nCount= 1;
	vector<ST_ZJCLS *>::iterator it;
	vector<ST_ZJCLS *>		m_ZJCls;
	//LoadZJCLS(m_ZJCls);

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;
	char szTxtPath[MAX_PATH] = {0};
	string strTxtPath = taskData->szLocDirectory;
	strTxtPath += "\\";
	strTxtPath += "201510";
	strcpy_s(szTxtPath,strTxtPath.c_str());
	f.GetFilesList( szTxtPath, "*.txt", strFileList );
	CUtility::Logger()->d("txt文件所在目录szLocPath[%s].",strTxtPath.c_str());
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	bool bInsert = false;

	int nLen = 4096;
	char *pBegin, *pEnd;
	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	char szFileName[16*1024]={0};
	//test
	//strFileNameList.clear();
	//strFileNameList.push_back( "s20131016.txt" );
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理txt文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		HS_MapStrToInt hashCptj;
		hashCptj.AllocMemoryPool();
		hashCptj.InitHashTable(4096);
		//LoadCptj(taskData,hashCptj);
		CUtility::Logger()->d("提取检索词列 begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				SetCurrentDirectory( szTxtPath );
				fp=fopen(vit->c_str(),"r");  
				if (!fp)
				{
					continue;
				}
				int i=0;  
				string sPreLine = "";
				string sCurLine = "";
				string sFileName = "";
				string strTmName = "";
				string strZtCode = "";
				string strSf = "";
				string strType = "";
				string strCurName = "";
				string::size_type   pos(0);     
				cChar=fgetc(fp);  
				while(!feof(fp))  
				{  
					try
					{
						if (cChar=='\r')
						{
							cChar=fgetc(fp);  
							continue;
						}
						if (cChar!='\n')
						{
							szLine[i]=cChar;  
						}
						else
						{
							szLine[i]='\0'; 
							strLine = szLine;

							if (strLine != "<REC>")
							{
								items.clear();
								CUtility::Str()->CStrToStrings(strLine.c_str(), items, '=');
								if (items.size()==2)
								{
									if ( items[1]!="")
									{
										if (items[0]=="<机标关键词>")
										{
											if (items[1].length() < 256)
											{
												strTmName = items[1];
											}
											else
											{
												strTmName = items[1].substr(0,255);
											}
										}
									}
								}
							}
							else
							{
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strTmName.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strCurName = items[j].c_str();
										if (strCurName.length() > 20)
										{
											strCurName = strCurName.substr(0,20);
										}
										if (strCurName.length() < 2)
										{
											continue;
										}
										strncpy( szKey, strCurName.c_str(), sizeof(szKey) );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}
									++nCount;
									if (nCount%100000 == 0)
									{
										CUtility::Logger()->d("read rec line[%d]",nCount);
									}
							}

							sPreLine = szLine;
							bInsert = true;
						}
						if (!bInsert)
						{
							++i;  
						}
						else
						{
							i = 0;
							memset( szLine, 0, sizeof(szLine) );
							bInsert = false;
						}
					}
					catch(exception ex)
					{
						CUtility::Logger()->d("exception .");

					}
					cChar=fgetc(fp);  
				}  
									memset(szFileName,0,sizeof(szFileName));
									strncpy( szFileName, strTmName.c_str(), sizeof(szFileName) - 1 );
									_strupr( szFileName );
									sFileName = szFileName;
									items.clear();
									CUtility::Str()->CStrToStrings(sFileName.c_str(),items,';');
									for (int j=0;j<items.size();j++)
									{
										memset(szKey,0,sizeof(szKey));
										strncpy( szKey, items[j].c_str(), sizeof(szKey) );
										if( hashCptj.Lookup( szKey, nptrVal ) )
										{
											hashCptj.SetAt( szKey, nptrVal + 1 );
										}
										else
										{
											hashCptj.SetAt( szKey, 1 );
										}		
									}

				if (fp) fclose(fp);
		}
		catch(exception e)
		{
			CUtility::Logger()->d("异常发生.");
		}
		}

		CUtility::Logger()->d("写入文件开始.");

		{
			strFileNew = taskData->szLocDirectory;
			strFileNew += "\\";
			strFileNew += "RESULT\\";
			strFileNew += "机标关键词词频.txt";
			::DeleteFile( strFileNew.c_str() );
			FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

			HITEM_POSITION pos;
			char *key;
			char szValue[32]={0};
			for( pos = hashCptj.GetStartPosition(); pos != NULL; )
			{
				hashCptj.GetNextAssoc( pos, key, nptrVal );
				itoa(nptrVal,szValue,10);
				fwrite( key, 1, (int)strlen(key), hFileNew );
				fwrite( "\t", 1, 1, hFileNew );
				fwrite( szValue, 1, (int)strlen(szValue), hFileNew );
				fwrite( "\n", 1, 1, hFileNew );
			}
			if (hFileNew) fclose(hFileNew);
		}
		CUtility::Logger()->d("写入条目文件结束.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的zip文件.");
	}

	return 0;
}

bool CKBConn::CheckDo(string strFileName)
{
	bool bResult = false;
	string strFileNameTemp = "";
	int j = _countof(gZj);
	for (int i = 0; i < _countof(gZj); ++i)
	{
		strFileNameTemp = gZj[i];
		strFileNameTemp = strFileNameTemp+"专辑全部排重词目.txt";
		if (strFileName == strFileNameTemp)
		{
			bResult = true;
		}
	}
	return bResult;
}

int CKBConn::GetTwoTxtComm(DOWN_STATE * taskData)
{
	long iCount= 0;
	long nCount= 1;
	int nptrVal;
	int nCurVal;
	char szKey[32];
	string strChar = "";
	map<string,int> mapValueSingle;
	map<string,int>::iterator mapit;

	char szLine[16*1024] = {0};
    FILE   *fp;  
    char   cChar;  
	string strLine = "";
	string strFile1 = "E:\\gpx\\687.txt";
	string strFile2 = "E:\\gpx\\1026.txt";
	string strFileNew = "E:\\GPX\\RESULT.TXT";
	::DeleteFile( strFileNew.c_str() );
	FILE *hFileNew = OpenRecFile( strFileNew.c_str() );

	bool bInsert = false;

	try
	{
		fp=fopen(strFile1.c_str(),"r");  
		if (!fp)
		{
			return 0;
		}
		int i=0;  
		string sCurLine = "";
		cChar=fgetc(fp);  
		while(!feof(fp))  
		{  
			try
			{
				if (cChar=='\r')
				{
					cChar=fgetc(fp);  
					continue;
				}
				if (cChar!='\n')
				{
					szLine[i]=cChar;  
				}
				else
				{
					szLine[i]='\0'; 
					strLine = szLine;
					mapValueSingle.insert(make_pair(strLine,1));

					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}

					bInsert = true;
				}
				if (!bInsert)
				{
					++i;  
				}
				else
				{
					i = 0;
					memset( szLine, 0, sizeof(szLine) );
					bInsert = false;
				}
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception .");
			}
			cChar=fgetc(fp);  
		}  

		if (fp) fclose(fp);

		fp=fopen(strFile2.c_str(),"r");  
		if (!fp)
		{
			return 0;
		}
		CUtility::Logger()->d("写入文件开始.");
		i=0;  
		sCurLine = "";
		cChar=fgetc(fp);  
		while(!feof(fp))  
		{  
			try
			{
				if (cChar=='\r')
				{
					cChar=fgetc(fp);  
					continue;
				}
				if (cChar!='\n')
				{
					szLine[i]=cChar;  
				}
				else
				{
					szLine[i]='\0'; 
					strLine = szLine;
					mapit = mapValueSingle.find(strLine);
					if (mapit != mapValueSingle.end())
					{
						fwrite( mapit->first.c_str(), 1, (int)strlen(mapit->first.c_str()), hFileNew );
						fwrite( "\n", 1, 1, hFileNew );
					}

					++nCount;
					if (nCount%100000 == 0)
					{
						CUtility::Logger()->d("read rec line[%d]",nCount);
					}

					bInsert = true;
				}
				if (!bInsert)
				{
					++i;  
				}
				else
				{
					i = 0;
					memset( szLine, 0, sizeof(szLine) );
					bInsert = false;
				}
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception .");
			}
			cChar=fgetc(fp);  
		}  
		CUtility::Logger()->d("写入条目文件结束.");

		if (fp) fclose(fp);
		if (hFileNew) fclose(hFileNew);
	}
	catch(exception e)
	{
		CUtility::Logger()->d("异常发生.");
	}
	return 0;
}


int CKBConn::WriteXmlToKbase(DOWN_STATE * taskData)
{
	vector<string> items;

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;

	f.GetFilesList( taskData->szLocPath, "*.xml", strFileList );
	CUtility::Logger()->d("xml文件所在目录szLocPath[%s].",taskData->szLocPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	int nRet = 0;
	char szSql[1024];

	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	string strType = "";
	char szFileName[1024]={0};
	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理xml文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		CUtility::Logger()->d("处理begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileNew = taskData->szLocPath;
				strFileNew += "\\";
				strFileNew += *vit;
				strFileName = *vit;
				strType = strFileName.substr(13,3);
				strFileName = strFileName.substr(0,13);
				strFileNew += "#DOBFILE";
				if (strType == "ENE")
				{
					sprintf( szSql, "update CJFDLAST2015  set E_FULLTEXT ='%s' WHERE 文件名='%s'", strFileNew.c_str(), strFileName.c_str());

			//	 update CJFDLAST2015 set E_FULLTEXT 
			//='C:\AJIP\ZGZE\ZGZE201501\ZGZE201501046ENE_FULLTEXT_CJFDLAST2015.XML#DOBFILE' 
			//	 where 文件名=ZGZE201501046

				}
				else
				{
					sprintf( szSql, "update CJFDLAST2015  set C_FULLTEXT ='%s' WHERE 文件名= '%s'", strFileNew.c_str(), strFileName.c_str());
				}
				nRet = ExecMgrSql( szSql );
				if( nRet < 0 )
				{
					CUtility::Logger()->d("exec [%s] result[%d]", szSql, nRet);
					return nRet;
				}
				Sleep(500);
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("处理  end.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的xml文件.");
	}

	return 0;
}

int CKBConn::RenameFileName(DOWN_STATE * taskData)
{
	vector<string> items;

	CFolder f;
	vector<string>strFileList;
	vector<string>strFileNameList;
	vector<string>::iterator it2;

	f.GetFilesList( taskData->szLocPath, "*.pdf", strFileList );
	CUtility::Logger()->d("pdf文件所在目录szLocPath[%s].",taskData->szLocPath);
	string strTxtFileName = "";
	for( it2 = strFileList.begin(); it2 != strFileList.end(); ++ it2 )
	{
			strTxtFileName = *it2;
			int pos;
			if( (pos=strTxtFileName.rfind("\\"))!=string::npos )
			{
				strTxtFileName = strTxtFileName.substr(pos+1,strTxtFileName.length());
				strFileNameList.push_back(strTxtFileName);
			}
	}
	int nptrVal;
	char szKey[32];

	int nRet = 0;
	char szSql[1024];

	string strFileName = "";
	vector<string>::iterator vit;
	map<hfsstring,hfsstring>::iterator cit;
	string strFilePath="";
	string strFileNew = "";
	string strName = "";

	int iFileCount = strFileNameList.size();
	CUtility::Logger()->d("处理pdf文件数[%d]",iFileCount);
	if (iFileCount > 0)
	{
		CUtility::Logger()->d("处理begin.");
		for( vit = strFileNameList.begin(); vit != strFileNameList.end(); ++ vit )
		{
			try
			{
				CUtility::Logger()->d("dowith file name.[%s]",vit->c_str());

				strFileName = taskData->szLocPath;
				strFileName += "\\";
				strFileName += *vit;
				strFileNew = taskData->szLocPath;
				strFileNew += "\\";
				strName = *vit;
				strName = strName.substr(0,13);
				strFileNew += strName;
				strFileNew += "_EN.pdf";
				BOOL bRes = ::MoveFile(strFileName.c_str(), strFileNew.c_str());
				if (!bRes){
					CUtility::Logger()->trace("Rename() error.%d,%s",GetLastError(), strFileName.c_str());
				}
			}
			catch(exception ex)
			{
				CUtility::Logger()->d("exception happen.");
			}
		}
		CUtility::Logger()->d("处理  end.");
	}
	else
	{
		CUtility::Logger()->d("没有要处理的xml文件.");
	}

	return 0;
}

int CKBConn::RunCapjGet(DOWN_STATE * taskData)
{

	int nRet = 0;
	int nRecordCount = 0;
	char szSql[1024] = {0};

	string strFileId = "";
	vector<string> fileIdList;
	vector<string>::iterator it;

	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d",systime.wYear,systime.wMonth,systime.wDay);

	CUtility::Logger()->d("插入文件名开始");
	sprintf( szSql, "select 文件名 from %s where 优先出版=Y+A", "CAPJLAST" );
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]","CAPJLAST",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*5);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	for (int i = 1;i <= nRecordCount;++i)
	{
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"文件名",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strFileId = t;
			fileIdList.push_back(strFileId);
		}

		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );

	try
	{
		CMSSQL sqlServer("");
		if( !sqlServer.OpenConn(taskData->szHost,
			taskData->szMsUser,
			taskData->szMsPwd,
			taskData->szDbName)){
				return false;
		}
		int iResult = 0;
		//sprintf(szSql,"insert into capjlastyall select * from capjlasty");
		//iResult = sqlServer.ExecSql(szSql);
		sprintf(szSql,"truncate table capjlasty");
		iResult = sqlServer.ExecSql(szSql);
		for( it = fileIdList.begin(); it != fileIdList.end(); ++ it )
		{
			strFileId = *it;
			sprintf(szSql,"insert into capjlasty(fileid,addtime) values('%s','%s')",strFileId.c_str(),cTime);
			iResult = sqlServer.ExecSql(szSql);
			if (iResult != 1)
			{
				CUtility::Logger()->d("insert data failed(%s)",strFileId.c_str());
			}
		}
	}
	catch(exception e)
	{
		CUtility::Logger()->d("exception happened");
	}
	CUtility::Logger()->d("插入文件名结束");
	return 0;
}

int CKBConn::RunCapjGetSf(DOWN_STATE * taskData)
{

	int nRet = 0;
	int nRecordCount = 0;
	char szSql[1024] = {0};

	string strFileId = "";
	vector<string> fileIdList;
	vector<string>::iterator it;

	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d",systime.wYear,systime.wMonth,systime.wDay);

	CUtility::Logger()->d("插入文件名开始");
	sprintf( szSql, "select 文件名 from %s where 标识=2", "CAPJLAST" );
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]","CAPJLAST",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*5);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	for (int i = 1;i <= nRecordCount;++i)
	{
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"文件名",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strFileId = t;
			fileIdList.push_back(strFileId);
		}

		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );

	try
	{
		CMSSQL sqlServer("");
		if( !sqlServer.OpenConn(taskData->szHost,
			taskData->szMsUser,
			taskData->szMsPwd,
			taskData->szDbName)){
				return false;
		}
		int iResult = 0;
		//sprintf(szSql,"insert into capjlastyall select * from capjlasty");
		//iResult = sqlServer.ExecSql(szSql);
		sprintf(szSql,"truncate table capjlastsf");
		iResult = sqlServer.ExecSql(szSql);
		for( it = fileIdList.begin(); it != fileIdList.end(); ++ it )
		{
			strFileId = *it;
			sprintf(szSql,"insert into capjlastsf(fileid,addtime,starttime,endtime) values('%s','%s','%s','%s')",strFileId.c_str(),cTime,"2017-11-01 00:00:00","2017-11-30 23:59:59");
			iResult = sqlServer.ExecSql(szSql);
			if (iResult != 1)
			{
				CUtility::Logger()->d("insert data failed(%s)",strFileId.c_str());
			}
		}
	}
	catch(exception e)
	{
		CUtility::Logger()->d("exception happened");
	}
	CUtility::Logger()->d("插入文件名结束");
	return 0;
}

int CKBConn::RunCapjGetLoop(DOWN_STATE * taskData)
{

	int nRet = 0;
	int nRecordCount = 0;
	char szSql[1024] = {0};

	string strFileId = "";
	vector<string> fileIdList;
	vector<string>::iterator it;

	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d",systime.wYear,systime.wMonth,systime.wDay);

	CUtility::Logger()->d("插入capjdaycurrent文件名开始");
	sprintf( szSql, "select 文件名 from %s", "CAPJDAY" );
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]","CAPJLAST",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*5);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	for (int i = 1;i <= nRecordCount;++i)
	{
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"文件名",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strFileId = t;
			fileIdList.push_back(strFileId);
		}

		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );

	try
	{
		CMSSQL sqlServer("");
		if( !sqlServer.OpenConn(taskData->szHost,
			taskData->szMsUser,
			taskData->szMsPwd,
			taskData->szDbName)){
				return false;
		}
		int iResult = 0;
		//sprintf(szSql,"insert into capjdayall select * from capjdaycurrent");
		//iResult = sqlServer.ExecSql(szSql);
		sprintf(szSql,"truncate table capjdaycurrent");
		iResult = sqlServer.ExecSql(szSql);
		for( it = fileIdList.begin(); it != fileIdList.end(); ++ it )
		{
			strFileId = *it;
			sprintf(szSql,"insert into capjdaycurrent(fileid,addtime) values('%s','%s')",strFileId.c_str(),cTime);
			iResult = sqlServer.ExecSql(szSql);
			if (iResult != 1)
			{
				CUtility::Logger()->d("insert data failed(%s)",strFileId.c_str());
			}
		}

		vector<_ParameterPtr> params;

		_ParameterPtr pParamTask;
		pParamTask.CreateInstance("ADODB.Parameter");
		pParamTask->Name="@fileids";
		pParamTask->Type=adVarChar;
		pParamTask->Size=50;
		pParamTask->Direction=adParamInput;
		pParamTask->Value = "test";

		params.push_back(pParamTask);

		_ParameterPtr pParamRows;
		pParamRows.CreateInstance("ADODB.Parameter");
		pParamRows->Name="@row";
		pParamRows->Type=adInteger;
		pParamRows->Size=4;
		pParamRows->Direction=adParamInputOutput;
		pParamRows->Value = 0;

		params.push_back(pParamRows);
		
		iResult = 0;
		m_ErrCode = sqlServer.ExecProc("sp_InsertCapjLatFromCapjdaycurrent",&params,iResult);

	}
	catch(exception e)
	{
		CUtility::Logger()->d("exception happened");
	}
	CUtility::Logger()->d("插入capjdaycurrent文件名结束");
	return 0;
}

int CKBConn::RunCapjGetLoopSf(DOWN_STATE * taskData)
{

	int nRet = 0;
	int nRecordCount = 0;
	char szSql[1024] = {0};

	string strFileId = "";
	vector<string> fileIdList;
	vector<string>::iterator it;

	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d",systime.wYear,systime.wMonth,systime.wDay);

	CUtility::Logger()->d("插入capjdaycurrentsf文件名开始");
	sprintf( szSql, "select 文件名 from %s where  标识=2", "CAPJDAY" );
	m_ErrCode = IsConnected();
	TPI_HRECORDSET hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	while( m_ErrCode == TPI_ERR_CMDTIMEOUT || m_ErrCode == TPI_ERR_BUSY )
	{
		CUtility::Logger()->d("超时 sql info.[在循环%s中  sql %s]","CAPJDAY",szSql);
		Sleep( 1000 );
		hSet = TPI_OpenRecordSet(m_hConn,szSql,&m_ErrCode);
	}
	if( !hSet || m_ErrCode < 0 )
	{
		char szMsg[MAX_SQL];
		sprintf( szMsg, "SQL:%s失败, ErrCode=%d", szSql, m_ErrCode );
		CUtility::Logger()->d("error info.[%s]",szMsg);
		return m_ErrCode;
	}
	nRecordCount = TPI_GetRecordSetCount( hSet );
	if (nRecordCount == 0)
	{
		CUtility::Logger()->d("执行[%s]，结果集为0",szSql);
	}
	m_ErrCode = TPI_SetRecordCount( hSet, GETRECORDNUM*5);
	TPI_MoveFirst( hSet );
	hfschar t[64] = {0};
	for (int i = 1;i <= nRecordCount;++i)
	{
		memset(t,0,64);
		m_ErrCode = TPI_GetFieldValueByName(hSet,"文件名",t,64);	
		if(t[0] == '\0'){
			TPI_MoveNext(hSet);
			continue;
		}
		else
		{
			strFileId = t;
			fileIdList.push_back(strFileId);
		}

		TPI_MoveNext(hSet);
	}
	TPI_CloseRecordSet( hSet );

	try
	{
		CMSSQL sqlServer("");
		if( !sqlServer.OpenConn(taskData->szHost,
			taskData->szMsUser,
			taskData->szMsPwd,
			taskData->szDbName)){
				return false;
		}
		int iResult = 0;
		//sprintf(szSql,"insert into capjdayall select * from capjdaycurrentsf");
		//iResult = sqlServer.ExecSql(szSql);
		sprintf(szSql,"truncate table capjdaycurrentsf");
		iResult = sqlServer.ExecSql(szSql);
		for( it = fileIdList.begin(); it != fileIdList.end(); ++ it )
		{
			strFileId = *it;
			sprintf(szSql,"insert into capjdaycurrentsf(fileid,addtime,starttime,endtime) values('%s','%s','%s','%s')",strFileId.c_str(),cTime,"2017-11-01 00:00:00","2017-11-30 23:59:59");
			iResult = sqlServer.ExecSql(szSql);
			if (iResult != 1)
			{
				CUtility::Logger()->d("insert data failed(%s)",strFileId.c_str());
			}
		}

		vector<_ParameterPtr> params;

		_ParameterPtr pParamTask;
		pParamTask.CreateInstance("ADODB.Parameter");
		pParamTask->Name="@fileids";
		pParamTask->Type=adVarChar;
		pParamTask->Size=50;
		pParamTask->Direction=adParamInput;
		pParamTask->Value = "test";

		params.push_back(pParamTask);

		_ParameterPtr pParamRows;
		pParamRows.CreateInstance("ADODB.Parameter");
		pParamRows->Name="@row";
		pParamRows->Type=adInteger;
		pParamRows->Size=4;
		pParamRows->Direction=adParamInputOutput;
		pParamRows->Value = 0;

		params.push_back(pParamRows);
		
		iResult = 0;
		m_ErrCode = sqlServer.ExecProc("sp_InsertCapjLatFromCapjdaycurrentsf",&params,iResult);

	}
	catch(exception e)
	{
		CUtility::Logger()->d("exception happened");
	}
	CUtility::Logger()->d("插入capjdaycurrent文件名结束");
	return 0;
}
