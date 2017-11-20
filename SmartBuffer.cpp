#include "StdAfx.h"
#include "SmartBuffer.h"
#include "Utility.h"

CSmartMemory::CSmartMemory(void)
{
	m_Data = 0;
	CUtility::Buffer()->GetBuff(m_Data);
	if(m_Data){
		memset(m_Data->GetPtr(),0x00,m_Data->GetSize());
	}
}


CSmartMemory::~CSmartMemory(void)
{
	if(m_Data){
		CUtility::Buffer()->FreeBuff(m_Data);
	}
}

void* CSmartMemory::GetPtr()
{
	if(m_Data){
		return m_Data->GetPtr();
	}
	return 0;
}

hfsint CSmartMemory::GetSize()
{
	if(m_Data){
		return m_Data->GetSize();
	}

	return 0;
}