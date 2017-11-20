#pragma once
#include "kfsbase.h"
#include "GLock.h"


struct HFS_DOB_HEADER
{
	hfsuint64	pos;
	hfsuint64	size;
	hfschar		version[8];
	hfschar		pvoid[64];
};

class CDOBFile:public CRefCounter
{
public:
	CDOBFile(hfsstring name);
	~CDOBFile();
	inline CVirtualFilePtr VPF(){ return m_dob;}
	hfsbool			Write(hfsstring local,hfsstring dob,hfsuint64& pos);
	hfsuint64		GetPos();
	void			InitHeader(HFS_DOB_HEADER* dob);
	void			RC4(void*data, hfssize size,hfssize pos); 
	void			RC4(void*data, hfssize size,hfssize pos,hfssize ptr); 
private:
	G_Lock			m_Lock;
	hfsuint64		m_pos;
	CVirtualFilePtr m_dob;
};

typedef Ptr<CDOBFile> CDOBFilePtr;

class CHFSBaseReader :public CRefCounter
{
public: 
	CHFSBaseReader();
	~CHFSBaseReader();
	CDOBFilePtr GetFile(hfsstring dev);
	hfsbool		ReleaseFile(hfsstring dev);
private:
	map<hfsstring,CDOBFilePtr>	m_dobs;
	G_Lock							m_Lock;
};

typedef Ptr<CHFSBaseReader> CHFSBaseReaderPtr;

class CHFSBaseWriter :public CRefCounter
{
public: 
	CHFSBaseWriter();
	~CHFSBaseWriter();
	CDOBFilePtr GetFile(hfsstring dev);
	hfsbool		ReleaseFile(hfsstring dev);
private:
	map<hfsstring,CDOBFilePtr>	m_dobs;
	G_Lock							m_Lock;
};

typedef Ptr<CHFSBaseWriter> CHFSBaseWriterPtr;

class CHFSBase : public CKFSBase
{
public:
	CHFSBase(void);
	~CHFSBase(void);

	virtual hfshandle	OpenFile(pchfschar pFullName, pchfschar mode);
	virtual hfsuint64	SeekFile(hfshandle hHandle,hfsint64 iOffset,hfsuint uiOrigin);

	virtual hfsuint		ReadFile(hfshandle hHandle,phfschar pBuffer,hfsuint uiSize);
	//virtual hfsuint		ReadFile(hfshandle hHandle,hfsint64 pos,phfschar pBuffer,hfsuint uiSize);
	virtual hfsuint		ReadFile(hfshandle hHandle,hfsint64 pos,phfschar pBuffer,hfsuint uiSize);
	virtual hfsbool	    Write2DOB(CKFSHanlePtr hHandle,hfsuint64& pos );

	virtual	hfsint		CommitData(CKFSHanlePtr pHandle);
	hfsstring			GetDOBName(CKFSHanlePtr pHandle);
	hfsstring			GetDOBName(string pTableName);
	virtual hfsint		CreateDir(pchfschar pDir);
	virtual hfsint		DisableDir(pchfschar pDir);
	virtual	hfsint		EnableDir(pchfschar pDir);
	virtual hfsint		DeleteDir(pchfschar pDir);
	virtual hfsint		WriteFile(hfshandle hHandle,pchfschar pBuffer,hfsuint uiSize,hfsint64 pos=-1);
};

