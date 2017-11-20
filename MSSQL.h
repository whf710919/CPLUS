#ifndef _MS_SQL_H_
//
#import "..\\lib\\msado15.dll" \
	no_namespace rename("EOF", "adoEOF")


//#import "C:\\Program Files\\Common Files\\System\\ado\\msado15.dll" \
//	no_namespace rename("EOF", "adoEOF")

#define _MS_SQL_H_
#pragma once
#include "stdafx.h"
#include <vector>
#include <string>
using namespace std;
class CMSSQL
{
public:
	CMSSQL(char *pTable);
	~CMSSQL();

	bool OpenConn(char *pHost, char *pUserID, char *pPassword, char *pDatabase);
	void CloseConn();

	_RecordsetPtr OpenRecordset(char *pSQL);
	int ExecSql(const char *pSQL);
	string GetMSColValue(_RecordsetPtr pRecordset, const char *pColName);
	string GetMSColValue(_RecordsetPtr pRecordset, int ColNum);
	int ExecProc(const char *pProc, const char *pParam1, const char *pParam2=NULL, const char *pParam3=NULL,const char *pParam4=NULL);
	_RecordsetPtr ExecProcRecordset(const char *pProc,vector<_ParameterPtr> *parameters,int &RetValue);
	int ExecProc(const char *pProc,vector<_ParameterPtr> *parameters,int &RetValue);

	int ExecProc(const char *pProc);
	char			m_szReference[256];
	_ConnectionPtr  m_pConnection;
	_CommandPtr     m_pCommand;
};

#endif
