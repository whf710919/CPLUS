#include "StdAfx.h"
#include "MSSQL.h"

CMSSQL::CMSSQL(char *pTable)
{
	CoInitialize( NULL );

	if( pTable && strlen(pTable) )
		strcpy( m_szReference, pTable );
	else
		strcpy( m_szReference, "reference" );
}

CMSSQL::~CMSSQL()
{
	CloseConn();
	CoUninitialize();
}

bool CMSSQL::OpenConn(char *pHost, char *pUserID, char *pPassword, char *pDatabase)
{
	char cDatabaseSource[1024];
	memset( cDatabaseSource, 0, sizeof(cDatabaseSource) );
		
	sprintf( cDatabaseSource, "Provider=SQLOLEDB.1;Persist Security Info=False;Data Source=%s;Initial Catalog=%s;User Id=%s;Password=%s;",
			 pHost, pDatabase,
	    	 pUserID, pPassword );

	try
	{
		m_pConnection.CreateInstance(__uuidof(Connection));
		m_pCommand.CreateInstance(__uuidof(Command));

		m_pConnection->ConnectionString = _bstr_t(cDatabaseSource);
		m_pConnection->Open( "", "", "", adConnectUnspecified );

	}
	catch( _com_error)
	{
		m_pConnection = NULL;
		return false;
	}

	return true;
}

void CMSSQL::CloseConn()
{
	
	if( m_pConnection )
	{
		m_pConnection->Close();
	
		m_pConnection = NULL;
	}
}

int CMSSQL::ExecSql(const char *pSQL)
{
	_variant_t vRecords;
	int nRecordsAffected = 0;

	try
	{
		m_pConnection->CursorLocation = adUseClient;
		m_pConnection->Execute( _bstr_t(pSQL), &vRecords, adExecuteNoRecords );
		nRecordsAffected = vRecords.iVal;
		//nRecordsAffected = 0;
	}
	catch(_com_error)
	{
		nRecordsAffected = -1;	
	}
	
	return nRecordsAffected;
}

_RecordsetPtr CMSSQL::ExecProcRecordset(const char *pProc,vector<_ParameterPtr> *parameters,int &RetValue)
{
	_CommandPtr pCmd;
	HRESULT hr = pCmd.CreateInstance(__uuidof(Command));
	if(FAILED(hr))
	{
		return -1;
	}
	for(int i = 0; i < (int)parameters->size();i++){
		pCmd->Parameters->Append((*parameters)[i]);
	}
	pCmd->CommandType = adCmdStoredProc;
	pCmd->CommandText = pProc;
	pCmd->ActiveConnection = this->m_pConnection;
	
	_RecordsetPtr hSet = NULL;
	_variant_t v1;
	try
	{
		hSet = pCmd->Execute(NULL,NULL,adCmdStoredProc);
	}
	catch(...)
	{
		
	}
	return hSet;
}

int CMSSQL::ExecProc(const char *pProc,vector<_ParameterPtr> *parameters,int &RetValue)
{
	_CommandPtr pCmd;
	HRESULT hr = pCmd.CreateInstance(__uuidof(Command));
	if(FAILED(hr))
	{
		return -1;
	}
	for(int i = 0; i < (int)parameters->size();i++){
		pCmd->Parameters->Append((*parameters)[i]);
	}
	pCmd->CommandType = adCmdStoredProc;
	pCmd->CommandText = pProc;
	pCmd->ActiveConnection = this->m_pConnection;
	
	_variant_t v1;
	try
	{
		pCmd->Execute(&v1,NULL,adCmdStoredProc);
	}
	catch(...)
	{
		
	}
	for(int i = 0; i < (int)parameters->size();i++){
		if( (*parameters)[i]->Direction == adParamOutput || (*parameters)[i]->Direction == adParamInputOutput){
			v1 = (*parameters)[i]->Value;
			break;
		}
	}
	RetValue = (int)v1;
	return  RetValue;
}

int CMSSQL::ExecProc(const char *pProc)
{
	_CommandPtr pCmd;
	HRESULT hr = pCmd.CreateInstance(__uuidof(Command));
	if(FAILED(hr))
	{
		return -1;
	}
	pCmd->CommandType = adCmdStoredProc;
	pCmd->CommandText = pProc;
	pCmd->ActiveConnection = this->m_pConnection;

	_variant_t v1;
	try
	{
		pCmd->Execute(&v1,NULL,adCmdStoredProc);
		//pCmd-
	}
	catch(...)
	{

	}
	return v1.intVal;
}

int CMSSQL::ExecProc(const char *pProc, const char *pParam1, const char *pParam2, const char *pParam3,const char *pParam4)
{
	_CommandPtr pCmd;
	HRESULT hr = pCmd.CreateInstance(__uuidof(Command));
	if(FAILED(hr))
	{
		return -1;
	}
	pCmd->CommandType = adCmdStoredProc;
	pCmd->CommandText = pProc;
	pCmd->ActiveConnection = this->m_pConnection;
	pCmd->Parameters->Append(pCmd->CreateParameter("@fids", adVarChar, adParamInput, 25*200,  _variant_t(pParam1)));
	pCmd->Parameters->Refresh();
	pCmd->Parameters->Item[_variant_t("@fids")]->Value = _variant_t(pParam1);

	_variant_t v1;
	try
	{
		pCmd->Execute(&v1,NULL,adCmdStoredProc);
	}
	catch(...)
	{

	}
	pCmd.Release();
	return v1.intVal;
}

_RecordsetPtr CMSSQL::OpenRecordset(char *pSQL)
{
	if( m_pConnection == NULL || m_pCommand == NULL || !pSQL )
		return NULL;

	_RecordsetPtr pRecordset = NULL; 
 	try
	{
		_variant_t vtNull;
		vtNull.vt = VT_ERROR;
		vtNull.scode = DISP_E_PARAMNOTFOUND;
		_variant_t vRowsAffected;
		
		m_pCommand->CommandTimeout = 10 * 60 * 1000;
		m_pCommand->CommandText = _bstr_t(pSQL);
		m_pCommand->CommandType = adCmdText;
		m_pCommand->ActiveConnection = m_pConnection;
		pRecordset = m_pCommand->Execute( &vRowsAffected, &vtNull, adCmdText );
	}
	catch( _com_error)
	{	
		pRecordset = NULL;
	}

	long pl;
	pRecordset->get_Status( &pl );
	if( pl < 0 )
	{
		pRecordset->Close();
		pRecordset = NULL;
	}

	return pRecordset;
}

string CMSSQL::GetMSColValue(_RecordsetPtr pRecordset, const char *pColName)
{
	string strRet = "";
	try
	{
		variant_t var = pRecordset->Fields->GetItem( pColName )->Value;
		if( var.vt != VT_EMPTY){
			strRet = (char *)(_bstr_t)var;
			//strRet.trim();
		}
	}
	catch(...)
	{
		strRet = "";
	}

	return strRet;
}

string CMSSQL::GetMSColValue(_RecordsetPtr pRecordset, int ColNum)
{
	string strRet = "";
	try
	{
		long count = pRecordset->Fields->Count;
		strRet = (char *)(_bstr_t)pRecordset->Fields->Item[ColNum]->Value;
		//str = str.Trim().MakeUpper(); 
	}
	catch(_com_error)
	{
		strRet = "";
	}

	return strRet;
}