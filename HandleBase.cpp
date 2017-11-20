#include "StdAfx.h"
#include "HandleBase.h"
#include "Error.h"

CHandleBase::CHandleBase(void)
{
	time(&m_ctime);
	m_hHandle = (hfshandle)this;
	m_Server="";
	m_Port=0;
	m_dwAddress = (hfshandle)this;
	m_dwMagicID = HFS_MEM_MAGIC_ID;
}

CHandleBase::CHandleBase(hfshandle hHandle, pchfschar pServer, hfsint Port)
{
	m_hHandle = hHandle;
	
	if(pServer){
		m_Server = pServer;
	}
	else{
		m_Server="";
	}
	m_Port = Port;
	time(&m_ctime);
	m_dwAddress = (hfshandle)this;
	m_dwMagicID = HFS_MEM_MAGIC_ID;
}

CHandleBase::CHandleBase(CRefCounterPtr smart)
{
	time(&m_ctime);
	m_hHandle = (hfshandle)this;
	m_Server="";
	m_Port=0;
	m_dwAddress = (hfshandle)this;
	m_dwMagicID = HFS_MEM_MAGIC_ID;
	m_SmartHandle = smart;
}

CHandleBase::CHandleBase(vector<hfsint>* handles)
{
	time(&m_ctime);
	m_hHandle = (hfshandle)this;
	m_Server="";
	m_Port=0;
	m_dwAddress = (hfshandle)this;
	m_dwMagicID = HFS_MEM_MAGIC_ID;
	m_Handles = *handles;
}

hfshandle CHandleBase::GetHandle()
{
	if(!IsPtr()){
		return 0;
	}
	Refresh();
	return m_hHandle;
}

hfsstring CHandleBase::GetServer()
{
	if(!IsPtr()){
		return "";
	}
	Refresh();
	return m_Server;
}

hfsint CHandleBase::GetPort()
{
	if(!IsPtr()){
		return 0;
	}
	Refresh();
	return m_Port;
}

hfsint CHandleBase::Close()
{
	hfsint e = _ERR_SUCCESS;
	return e;
}

hfsbool	CHandleBase::IsTimeOut()
{
	if(!IsPtr()){
		return true;
	}
	time_t now;
	time(&now);
	if( now - m_ctime >= (HANDLE_TIMEOUT_SEC)){
		return true;
	}
	return false;
}

void CHandleBase::Refresh()
{
	if(!IsPtr()){
		return;
	}
	time(&m_ctime);
}

CHandleBase::~CHandleBase(void)
{
	m_dwAddress = HFS_INVALIDATE_ID;
	m_dwMagicID = HFS_INVALIDATE_ID;
	Close();
}

hfsbool CHandleBase::IsPtr()
{
	if(!this){
		return false;
	}

	return (HFS_MEM_MAGIC_ID == m_dwMagicID) && 
		((hfssize)this == m_dwAddress);
}

CHandleBaseMap::CHandleBaseMap(hfsint maxSize)
	:m_MaxSize(maxSize)
{
}

hfshandle CHandleBaseMap::AddHandle(void* ptr)
{
	G_Guard Lock(m_Lock);
	CHandleBasePtr pHandle = (CHandleBase*)(ptr);
	hfshandle hHandle = (hfshandle)*pHandle;
	map<hfssize,CHandleBasePtr>::iterator it;
	vector<hfssize> timeout;
	vector<hfssize>::iterator itt;

	if( (hfsint)m_HandleCache.size() > m_MaxSize){
		for ( it = m_HandleCache.begin(); it != m_HandleCache.end(); ++it)
		{
			if( it->second->IsPtr()){
				if( it->second->IsTimeOut()){
					//m_HandleCache.erase(it);break;
					timeout.push_back(it->first);
				}
			}else{
				//m_HandleCache.erase(it);break;
				timeout.push_back(it->first);
			}
		}
	}

	for(itt = timeout.begin(); itt != timeout.end(); ++itt){
		it = m_HandleCache.find(*itt);
		if(it != m_HandleCache.end()){
			m_HandleCache.erase(it);
		}
	}

	m_HandleCache.insert(make_pair((hfssize)hHandle,pHandle));

	return hHandle;
}

CHandleBasePtr CHandleBaseMap::GetHandle(hfshandle hHandle)
{
	G_Guard Lock(m_Lock);
	map<hfssize,CHandleBasePtr>::iterator it;
	it = m_HandleCache.find((hfssize)hHandle);
	if( it != m_HandleCache.end()){
		CHandleBasePtr pHandle =  it->second;
		if( pHandle->IsPtr()){
			return pHandle;
		}
		m_HandleCache.erase(it);
	}
	return 0;
}

void CHandleBaseMap::RemoveHandle(hfshandle hHandle)
{
	G_Guard Lock(m_Lock);
	map<hfssize,CHandleBasePtr>::iterator it;
	it = m_HandleCache.find((hfssize)hHandle);
	if( it != m_HandleCache.end()){
		m_HandleCache.erase(it);
	}
}

CHandleBaseMap::~CHandleBaseMap()
{
	m_HandleCache.clear();
}