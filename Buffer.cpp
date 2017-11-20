#include "StdAfx.h"
#include "buffer.h"


CBuffer::CBuffer(int nType)
{
	m_nSize = 0;
	m_nBufferType = nType;
	m_pPtr = m_pBase = NULL;
}

CBuffer::~CBuffer()
{
	if( m_pBase )
		VirtualFree(m_pBase,0,MEM_RELEASE);
	m_pPtr = m_pBase = NULL;
	m_nSize = 0;
}

bool CBuffer::Write(char *pData, int nSize)
{
	if( !pData || nSize <= 0 )
		return false;

	if(ReAllocateBuffer( nSize + GetBufferLen() + 1) == NULL)
		return false;

	CopyMemory( m_pPtr,pData,nSize );
	m_pPtr[nSize] = '\0';
	m_pPtr += nSize;
	return true;
}

char *CBuffer::ReAllocateBuffer(int nRequestedSize)
{
	if( m_pBase!=NULL && nRequestedSize <= GetMemSize() )
		return m_pBase;

	int nNewSize = ((nRequestedSize+1023)>>10)<<10;
	char *pNewBuffer = (char *) VirtualAlloc( NULL, nNewSize, MEM_COMMIT, PAGE_READWRITE );
	if(pNewBuffer==NULL)
		return NULL;
	pNewBuffer[0] = '\0';

	int nBufferLen = GetBufferLen();
	if(m_pBase != NULL)
	{
		if( nBufferLen > 0 )
		{
			CopyMemory( pNewBuffer, m_pBase, nBufferLen );
			pNewBuffer[nBufferLen] = '\0';
		}
		VirtualFree( m_pBase, 0, MEM_RELEASE );
	}
	m_pBase = pNewBuffer;
	m_pPtr = m_pBase + nBufferLen;
	m_nSize = nNewSize;
	return m_pBase;
}

char *CBuffer::DeAllocateBuffer(int nRequestedSize)
{
	if( m_pBase && nRequestedSize == GetMemSize() )
		return m_pBase;

	if( m_pBase != NULL)
	{
		VirtualFree( m_pBase, 0, MEM_RELEASE );
		m_pBase=NULL;
		m_pPtr=NULL;
		m_nSize=0;
	}

	int nNewSize = ((nRequestedSize+1023)>>10)<<10;
	char *pNewBuffer = (char *)VirtualAlloc( NULL, nNewSize, MEM_COMMIT, PAGE_READWRITE );
	if(pNewBuffer==NULL)
		return NULL;
	pNewBuffer[0] = '\0';

	m_pBase = pNewBuffer;
	m_pPtr = m_pBase;
	m_nSize = nNewSize;
	return m_pBase;
}