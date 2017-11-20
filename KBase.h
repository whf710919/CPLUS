#ifndef HFS_KBASE_H_
#define HFS_KBASE_H_ 

#ifdef _WIN32
#include "x86\\TPIClient.h"
#else
#include "x64\\TPIClient.h"
#endif

#include "smartptr.h"
#include "DataDef.h"
#include "BuffMgr.h"
#include "GLock.h"
#include "Markup.h"
#include "string.h"
using namespace std;

#define WAIT_TIMEOUT_SEC (60)
#define DEFAULT_KPORT	(4567)

struct SYSTEM_TABLE_FIELD_STYLE
{
	hfschar	name[HS_FIELD_NAME_LEN];
	HSINT32	type;
	HSINT32	len;
	HSINT32	indexkeyLen;
	HSINT32	indexType;
};

class CKBConn:public CRefCounter
{
public:
	CKBConn(pchfschar pSvrIP, hfsuint uiPort);
	virtual ~CKBConn();
	hfsint				Clear();
	hfsbool				TableExist(pchfschar pTable);
	hfsint				CreateTable(pchfschar pTable,SYSTEM_TABLE_FIELD_STYLE * tfs);
	hfsint				CreateView(pchfschar pTable);
	hfsint				DeleteTable(pchfschar pTable,hfsbool bLogic=false);
	hfsint				ImportTable(pchfschar pTable,pchfschar pDbName=0);
	hfsint				DisableTable(pchfschar pTable);
	hfsint				EnableTable(pchfschar pTable);
	hfsint				GetTableInfo(pchfschar pTable,HFS_DIR_INFO *pDir);
	hfsint				GetTableInfoX(pchfschar pTable,HFS_DIR_INFO *pDir);
	hfsbool				IsExist(pchfschar pFullName);
	hfsint				OpenFile(pchfschar pFullName);
	hfsint				OpenTableFile(pchfschar pFullName);
	hfsint				FindFile(pchfschar pFullName);
	hfsint				OpenDir(pchfschar pFullName);
	hfsint				IndexDir(pchfschar pFullName);
	hfsbool				MoveNext();
	hfsbool				MovePrev();
	hfsbool				MoveFirst();
	hfsbool				MoveLast();
	hfsbool				Move(hfsint pos);
	hfsuint64			GetFileCount();
	hfsint				DeleteFileKbase(pchfschar pFullName);
	hfsint				GetFileInfo(HFS_FILE_INFO *fd);
	hfsint				GetDirFiles(hfsint pos,hfsint size,vector<hfsstring>* files);
	hfststring			GetFileTable(pchfschar pFullName);
	hfsstring			GetFindTable(pchfschar pFullName);
	hfsstring			GetFindTableX(pchfschar pFullName,bool& bFuzzy);
	hfststring			GetFileImportPath(pchfschar pFullName);

	hfststring			GetFileName(pchfschar pFullName);
	hfststring			GetFileNameX(pchfschar pFullName);
	hfststring			GetFileExtName(pchfschar pFullName);
	hfsint				WriteFile(hfststring pFullName);
	hfsint				WriteFile(hfststring pFullName,hfsuint64 pos);
	hfsint				ReadFile(phfschar pBuffer,hfssize iOffset,hfsint iByte);
	hfststring			Local2DcsDirectory(pchfschar pDirName);
	hfststring			Dcs2LocalDirectory(pchfschar pDirName,hfsbool bView=false);
	hfststring			Dcs2LocalSyncDirectory(pchfschar pDirName,hfsbool bView=false);
	hfsstring			Dcs2LocalTableName(pchfschar pDirName,hfsbool bView = false);
	hfsint				GetDbTables(pchfschar dbName,vector<string>* tables);
	hfsbool				IsTableLock(pchfschar pTable);
	void				KTime2Systime(pchfschar ktime,phfschar systime);
	hfsbool				IsFindDir();
	hfsint				IsConnected();
	hfsstring			TryGetDevPath(pchfschar pTableName);
	hfsstring			GetRootNameByPath(pchfschar pDevName);
	hfsstring			GetDevPath(pchfschar pTableName);
	hfsstring			GetDevPath();
	hfsstring			GetDevPathX(pchfschar pTablePath);
	hfsint				GetAllTables(vector<string>& tables);

	hfsint				ChangeNodeID(HFS_DIR_INFO* dir);
	hfsint				WriteRecord(DOWN_STATE * taskdata);
	hfsint				SetStatus(hfsint iStatus,TASKDATA * taskdata,hfsstring strComment);
	hfsbool				UpdateQuery(phfschar sql,TASKDATA * taskdata,hfsstring &strTableFirst);
	hfsbool				ParserFormat(CMarkup &xParser,vector<GROUPMATCH*> &groups);
	hfsbool				LoadGroup(CMarkup &xParser, GROUPMATCH *gm);
	void				AddGroup(GROUPMATCH *group,vector<GROUPMATCH*> &groups);
	void				AddDataBase(GROUPMATCH *gm,DATABASEMATCH *database);
	string				ParserName(CMarkup &xParser);
	hfsint				ExecuteCmd(pchfschar pSql);
	hfsint				ExecuteCmdTask(pchfschar pSql);
	string				ParserPreWord(CMarkup &xParser);
	string				ParserPostWord(CMarkup &xParser);
	string				ParserFileSaveMode(CMarkup &xParser);
	string				ParserBaseUrl(CMarkup &xParser);
	string				ParserTableID(CMarkup &xParser);
	hfsbool				LoadDataBase(CMarkup &xParser, DATABASEMATCH *dm);
	string				ParserShowStyle(CMarkup &xParser);
	hfsbool				LoadMaterSubMatch(CMarkup &xParser, MATERSUBMATCH *msm);
	void				AddMaterSubMatch(DATABASEMATCH *dm,MATERSUBMATCH *msm);
	string				ParserHaveSubField(CMarkup &xParser);
	string				ParserSource(CMarkup &xParser);
	string				ParserDest(CMarkup &xParser);
	string				ParserType(CMarkup &xParser);
	hfsbool				LoadMatch(CMarkup &xParser, MATCH *match);
	void				AddMatch(MATERSUBMATCH *msm,MATCH *subPair);
	string				ParserID(CMarkup &xParser);
	void				SetGroup(GROUPMATCH *group);
	DATABASEMATCH		*GroupMatchCotainDBCode(GROUPMATCH *group, string tableName);
	hfsstring			GetFieldValueByName(TPI_HRECORDSET &rs, string sFieldName);
	BOOL 				ExecuteProcess(LPCTSTR cmd, LPCTSTR param, HANDLE& hProcess);
	hfsbool				LoadZJCLS(vector<ST_ZJCLS*> &m_ZJCls);
	hfsbool				LoadAuthor(vector<ST_AUTHOR*> &m_vAuthor);
	hfsbool				LoadHosp(vector<ST_HOSP*> &m_Hosp);
	hfsbool				LoadUniv(vector<ST_UNIV*> &m_Univ);
	hfsbool				LoadCptj(DOWN_STATE * taskData, HS_MapStrToInt &m_Cptj);
	hfsbool				CheckData(string strSourceTable,string strTargetTable);
	void				InsertQuery(phfschar sql,string strTable,string strAuthorCode,string strAllAuthorCode,string strAuthorName,string strJg,string strZtdm,string strDw,int iNum);
	hfsbool				CheckDataUniv(string strSourceTable,string strTargetTable);
	hfsint				InsertQueryUniv(phfschar sql,string strTable,string strJgCode,string strAllJgCode,string strJgName,int iNum);
	hfsint				InsertQueryUnivAll(string strTableSouce,string strTableTarget,string strJgCode,string strJgName,string strNum);
	hfsbool				CheckDataHospUniv(string strSourceTable,string strTargetTable);
	hfsint				InsertQueryUniv(phfschar sql,string strTable,string strJgCode,string strAllJgCode);

	hfsint				UpdateQuery(string strTable,string strAuthorCode,string strAllAuthorCode,string strZtdm,int iCount);
	hfsbool				AppendData(string strSourceTable,string strTargetTable);
	hfsint				AddVector(TPI_HRECORDSET &hSet,vector<hfsstring> &vector,hfsint iCountNum);
	int					RunQIKA(DOWN_STATE * taskData);
	int					RunAUTH(DOWN_STATE * taskData);
	int					RunHOSP(DOWN_STATE * taskData);
	int					RunUNIV(DOWN_STATE * taskData);
	int					RunZTCS(DOWN_STATE * taskData);
	int					RunDOCU(DOWN_STATE * taskData);
	int					RunDICT(DOWN_STATE * taskData);
	int					RunSTAT(DOWN_STATE * taskData);
	int					RunDOWNLOAD(DOWN_STATE * taskData);
	int					RunCOLUHIER(DOWN_STATE * taskData);

	FILE				*OpenRecFile(const char *pTable);
	int					CloseRecFile(FILE *pFile);
	
	int					WriteRecFile(FILE *pFile, char *pFieldName, const char *pData, bool bSameRow);
	int					WriteRecFile(FILE *pFile, char *pFieldName, int nNum, bool bSameRow);

	int					WriteRecFileDOCU(FILE *pFile, ST_DOCU *pItem);
	bool				IsValidata(char *pName, char *pTable);
	int					GetTimes(const char *pSql);
	int					GetHIndex(const char *pSql);
	int					AddVectorAuthor();
	int					AddVectorUniv(char * szTable);
	hfsint				AddMapAuthor(TPI_HRECORDSET &hSet,map<hfsstring,hfsstring> &map,hfsint iCountNum);
	hfsint				AddMapUniv(TPI_HRECORDSET &hSet,map<int,hfsstring> &map,hfsint iCountNum);
	hfsint				AddMapHosp(TPI_HRECORDSET &hSet,map<hfsstring,hfsstring> &map,hfsint iCountNum);
	int					StatNew(DOWN_STATE * taskData, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere = NULL );
	int					StatNewUpdate(DOWN_STATE * taskData, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere = NULL );
	hfsint				AddMapAuthorCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable);
	hfsint				AddMapAuthorCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,string> &map,hfsint iCountNum,char * pTable);
	hfsint				AddMapUnivCitedNum(TPI_HRECORDSET &hSet,map<hfsstring,string> &map,hfsint iCountNum,char * pTable,std::map<hfsstring,int> &Curmap);
	int					UpdateTable(char *pTable, const char *pRecFile);
	int					UpdateTableTrue(char *pTable, const char *pRecFile, const char *pKey);
	int					UpdateFiledValue(char *pTable);
	int					UpdateFiledValueYear(char *pTable,int nYear);
	int					SplitRec(map<hfsstring,int> &map, const char *pRecFile, const char *pRecFileAdd, const char *pRecFileUpdate);
	int					GetMap(char *pTable, const char *pKey, map<hfsstring,int> &map, int nyear);
	bool				IsZeroSize(const char *pRecFile);

	int					SortTable(int nType);
	int					ExecSql(char *pSql);
	int					ExecMgrSql(char *pSql, bool bWait = true);
	hfsint				AddMapAuthorCitedNumMulti(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable);
	hfsint				AddMapUnivCitedNumMulti(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable);
	hfsint				AddMapHospCitedNumMulti(TPI_HRECORDSET &hSet,map<hfsstring,int> &map,hfsint iCountNum,char * pTable);
	int					WriteRecFileFloat(FILE *pFile, char *pFieldName, float nNum, bool bSameRow);
	int					AddAuthorMulti(char * pZtCode, char * pTable, char *pWhere, char *pField, char *pCodeName, char *pDict);
	int					AddUnivMulti(char * pZtCode, char * pTable, char *pWhere, char *pField, char *pCodeName, char *pDict, char *pSourceTable);
	int					AddUnivItem(char * pZtCode, char * pTable, string strWhere, char *pField, char *pCodeName, char *pDict);
	int					AddAuthItem(char * pZtCode, char * pTable, string strWhere, char *pField, char *pCodeName, char *pDict);
	int					GetResult(const char *pSql,string &strResult);
	int					UpdateTableHosp(string strNumber,string strValue,string strTable);
	int					StatDoc(DOWN_STATE * taskData, char *pRetFile);
	int					StatDocUpdate(DOWN_STATE * taskData, char *pRetFile);
	
	string				GetUpFileName(const char *pTable);
	int					AddUnivItem(char * szSql,map<string,int> &map,std::map<string,int> &univ);
	int					AddAuthItem(char * szSql,map<string,int> &map);
	hfsint				GetItemsNumber(char * szCode,map<hfsstring,int>&map,long long &nResult);
	int					GetTimes2(const char *pSql,long long &item0,long long &item1);
	void				UniqVectorItem(vector<pair<string,int>>& sVector,vector<pair<string,int>>& tVector);
	int					RunDownLoad(DOWN_STATE * taskData);
	int					RunDownLoadTrue(DOWN_STATE * taskData);
	int					RunTextGet(DOWN_STATE * taskData);
	int					RunTextGetTrue(DOWN_STATE * taskData);
	int					RunTextGetTrue1(DOWN_STATE * taskData);
	int					RunTextGetTrue2(DOWN_STATE * taskData);
	int					RunTextGetTrue3(DOWN_STATE * taskData);
	int					RunTextGetTrue4(DOWN_STATE * taskData);
	hfsbool				LoadCpFromFile(const char * szFileName, HS_MapStrToInt &hashCptj);
	int					RunTextGetTrueTm(DOWN_STATE * taskData);
	int					RunTextGetTrueTm5(DOWN_STATE * taskData);
	int					RunTextGetTrueTm7(DOWN_STATE * taskData);
	int					RunTextGetTrueTm8(DOWN_STATE * taskData);
	int					RunTextGetTrueTm9(DOWN_STATE * taskData);
	int					RunTextGetTrueTm10(DOWN_STATE * taskData);
	int					RunTextGetTrueTm11(DOWN_STATE * taskData);
	bool				CheckDo(string strFileName);
	int                 GetTwoTxtComm(DOWN_STATE * taskData);
	int					WriteXmlToKbase(DOWN_STATE * taskData);
	int					RenameFileName(DOWN_STATE * taskData);

	int					RunCapjGet(DOWN_STATE * taskData);
	int					RunCapjGetLoop(DOWN_STATE * taskData);
	int					RunCapjGetSf(DOWN_STATE * taskData);
	int					RunCapjGetLoopSf(DOWN_STATE * taskData);

	int					TextToSql();
	int					ExecTextToKbase(DOWN_STATE * taskData);
	int					ExecFieldUpdate(DOWN_STATE * taskData);
	int					LoadCJFDBaseinfo(map<string,string>&pykmMap);
	bool				IsCJFD(map<hfsstring,int> * code, const char *pFileName, const char *pCode);
	bool				IsCDMD(map<hfsstring,int> * code, const char *pFileName, const char *pCode);
	bool				IsCPFD(map<hfsstring,int> * code, const char *pFileName, const char *pCode, map<hfsstring,int> * checkCode);
	int					GetTop1Value(const char *pSql);
	bool				GetPubYear(const char *pFileName, char *szPubYear);
	int					GetMonthDays(int y, int m);
	int					GetLocalFiles(DOWN_STATE * taskData);
	int					AppendTable(char *pTable, const char *pRecFile);
	int					GetDifferFile(DOWN_STATE * taskData);
	int					StatZt(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere);
	int					StatColumn(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere);
	int					StatColumn_NQ(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere);
	int					StatColumn_168(DOWN_STATE * taskData, char *pTable, char *pField, char *pCodeName, char *pDict, char *pRetFile, char *pField2, char *pWhere);
	hfsbool				LoadAllZJCLS(vector<ST_ZJCLS*> &m_AllZJCls);
	hfsbool				LoadZJCLSMap(map<hfsstring,hfsstring> &m_ZJCls);
	hfsbool				LoadPykm(vector<ST_PYKM*> &m_AllPykm);
	hfsbool				LoadPykmHier(vector<ST_PYKM*> &m_AllPykm);
	hfsbool				LoadPykmHierNq(vector<ST_PYKM*> &m_AllPykm);
	void				InsertLine();
	int					LoadFieldInfo(const char * szSql, std::vector<string>&vCodeUniqCode);
	int					WriteRecFileDOCUUpdate(FILE *pFile, ST_DOCU *pItem);
protected:
	hfsint	OpenConn();
	void	CloseConn();

	hfsint	Query(pchfschar pSql);
	
	hfsint	GetErrorCode();
	hfsint	QueryEvent(TPI_HEVENT ev);
	hfststring GetTablePath(pchfschar pTable);
	hfsuint64	GetDirSize(pchfschar pTable);
	hfsbool		IsViewExist(hfsstring pView);
	hfsbool		IsRootExist(hfsstring pRoot);
	hfsint		CreateRoot(hfsstring pRoot);
	hfststring	GetFileView();
	hfsint		FormatTable(SYSTEM_TABLE_FIELD_STYLE * tfs,HS_TABLE_FIELD *ptf);
	hfsstring	GetOpenString(pchfschar pFileName,pchfschar pTables = 0);
	
	hfsstring	GetFindString(pchfschar pFileName);
	hfsstring   GetFindStringWithOutKey(pchfschar pFileName);
	static void Trim(hfschar* pText,hfschar ch);
private:
	hfsint			m_ErrCode;
	hfsuint			m_nPort;
	hfslong			m_lCount;
	hfschar			m_szIP[16];
	TPI_HCON		m_hConn;
	TPI_HRECORDSET	m_hSet;
	G_Lock			m_Lock;
	GROUPMATCH		*m_group_info;
	map<hfsstring,hfsint>	m_FileNameRecordNoCite;
	map<hfsstring,hfsint>	m_FileNameRecordNoCited;
	hfsbool m_bExcept;
	vector<hfsstring> m_FileNameExcept;
	hfsstring m_strSqlAll;
	map<hfsstring,hfsstring>	m_FileNameDataCite;
	map<hfsstring,hfsstring>	m_FileNameDataCited;
  	OUTFILENAME m_OutFileName;
	int m_ReUpData;
	map<hfsstring,hfsstring> m_ZtCodeAuthorCode;
	map<hfsstring,int> m_AuthorCodeCitedNum;
	map<hfsstring,hfsstring> m_AuthorCodeCitedNumMiddle;
	char m_szZSK[64];
	map<int,hfsstring> m_UnivCodeMulti;
	map<hfsstring,int> m_UnivZwl;
	map<hfsstring,int> m_UnivXzl;
	map<hfsstring,int> m_UnivHx;
	map<hfsstring,int> m_UnivSci;
	map<hfsstring,int> m_UnivEi;
	map<hfsstring,int> m_UnivFun;
	map<hfsstring,DWORD>	m_DownloadFileInfo;
	map<hfsstring,int> m_CurUnivCode;
	vector<hfsstring> m_vCodeUniqCode;
};

typedef Ptr<CKBConn> CKBConnPtr;


class CDirFactory : public CRefCounter
{
public:
	CDirFactory();
	~CDirFactory();
	hfsbool		Init();
	hfsbool		DirExist(pchfschar pTable);
	void		CreateDir(pchfschar pTable,HFS_DIR_INFO* dir=0);
	void		DeleteDir(pchfschar pTable);
	hfsbool		GetDirCacheInfo(pchfschar pTable,HFS_DIR_INFO* dir);
private:
	G_Lock		m_Lock;
	CKBConnPtr	m_Con;
	map<hfsstring,HFS_DIR_INFO*>  m_dirs;
};

typedef Ptr<CDirFactory> CDirFactoryPtr;

#endif 