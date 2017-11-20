#include "StdAfx.h"
#include "KFSBase.h"
#include "Error.h"
#include "Utility.h"
#include "RecFile.h"
#include "SmartBuffer.h"

#define  CACHE_UNIT_SIZE (100000)
#define  MAX_HANDLE_USE	 (10240)

vector<ST_ZJCLS *>		m_ZJCls;
vector<ST_AUTHOR *>		m_Author;
vector<ST_HOSP *>		m_Hosp;
vector<ST_UNIV *>		m_Univ;



CKFSBase::CKFSBase(void)
{
}


CKFSBase::~CKFSBase(void)
{
}

CKBConnPtr CKFSBase::GetConn()
{
	CKBConnPtr pCon = new CKBConn("127.0.0.1",DEFAULT_KPORT);
	int e = pCon->IsConnected();
	if( e < _ERR_SUCCESS){
		//CUtility::Logger()->trace("Node [%s] IsConnected() error[%d]",CUtility::Config()->GetLocalNodeInfo()->IP,e);
		return 0;
	}
	return pCon;
	/*G_Guard Lock(m_Lock);
	CKBConnPtr pCon = 0;
	if( !m_ConPools.empty()){
		pCon = m_ConPools.front();
		m_ConPools.pop();
	}else{
		pCon = new CKBConn("127.0.0.1",DEFAULT_KPORT);
	}
	return pCon;*/
}

CKBConnPtr CKFSBase::GetConn(SYSTEMINFO * m_systemInfo)
{
	if (0 == m_systemInfo->nPort)
	{
		m_systemInfo->nPort = DEFAULT_KPORT;
	}
	//现在用的是这个，别弄错了
	string strIp = m_systemInfo->szIp;
	if (strIp == "")
	{
		strIp = "127.0.0.1";
	}

	//CKBConnPtr pCon = new CKBConn("192.168.103.37",m_systemInfo->nPort);
	//CKBConnPtr pCon = new CKBConn("192.168.100.157",m_systemInfo->nPort);
	CKBConnPtr pCon = new CKBConn(strIp.c_str(),m_systemInfo->nPort);
	//CKBConnPtr pCon = new CKBConn("127.0.0.1",m_systemInfo->nPort);
	//CKBConnPtr pCon = new CKBConn("192.168.100.211",m_systemInfo->nPort);
	int e = pCon->IsConnected();
	if( e < _ERR_SUCCESS){
		//CUtility::Logger()->trace("Node [%s] IsConnected() error[%d]",CUtility::Config()->GetLocalNodeInfo()->IP,e);
		return 0;
	}
	return pCon;
}

void CKFSBase::ReleaseConn(CKBConnPtr pCon)
{
	/*G_Guard Lock(m_Lock);
	if(pCon && (hfsuint)m_ConPools.size() < MAX_CONN_COUNT){
		pCon->Clear();
		m_ConPools.push(pCon);
	}
	*/
}

hfsint	CKFSBase::ReadRecord(vector<TASKDATA*>& taskdata)
{
	hfsint e = _ERR_SUCCESS;

	return e;
}
hfsint	CKFSBase::ReadRecordLoop(vector<TASKDATA*>& taskdata)
{
	hfsint e = _ERR_SUCCESS;

	return e;
}

hfsbool	CKFSBase::SetStatus(hfsint64 id,hfsint status)
{
	return true;
}
hfsbool	CKFSBase::WriteResult(hfsint64 id,pchfschar pBuffer,hfsint64 recCount,hfsint64 citeCount,hfsint64 cited_count)
{
	return true;
}

hfsint	CKFSBase::DoWriteRecord(DOWN_STATE* taskdata)
{
	hfsint e = _ERR_SUCCESS;

	CKBConnPtr pCon = GetConn(m_systemInfo);
	if(pCon){
		e = pCon->WriteRecord(taskdata);
		ReleaseConn(pCon);
	}else{
		CUtility::Logger()->trace("kbase must be running,please check kbase then service restart");
		e = _ERR_KBASE_CONN;
	}
	return e;
}

hfsbool	CKFSBase::LoadZJCLS()
{
	hfsint e = _ERR_SUCCESS;

	CKBConnPtr pCon = GetConn(m_systemInfo);
	if(pCon){
		e = pCon->LoadZJCLS(m_ZJCls);
		ReleaseConn(pCon);
	}else{
		e = _ERR_KBASE_CONN;
	}
	return (e > 0) ? true:false;
}

hfsbool	CKFSBase::LoadAuthor()
{
	hfsint e = _ERR_SUCCESS;

	CKBConnPtr pCon = GetConn(m_systemInfo);
	if(pCon){
		e = pCon->LoadAuthor(m_Author);
		ReleaseConn(pCon);
	}else{
		e = _ERR_KBASE_CONN;
	}
	return (e > 0) ? true:false;
}

hfsbool	CKFSBase::LoadHosp()
{
	hfsint e = _ERR_SUCCESS;

	CKBConnPtr pCon = GetConn(m_systemInfo);
	if(pCon){
		e = pCon->LoadHosp(m_Hosp);
		ReleaseConn(pCon);
	}else{
		e = _ERR_KBASE_CONN;
	}
	return (e > 0) ? true:false;
}

hfsbool	CKFSBase::LoadUniv()
{
	hfsint e = _ERR_SUCCESS;

	CKBConnPtr pCon = GetConn(m_systemInfo);
	if(pCon){
		e = pCon->LoadUniv(m_Univ);
		ReleaseConn(pCon);
	}else{
		e = _ERR_KBASE_CONN;
	}
	return (e > 0) ? true:false;
}
