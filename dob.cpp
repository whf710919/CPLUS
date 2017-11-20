#include "StdAfx.h"
#include "dob.h"



#define buf_size_transfer (1024*1024*10)

CDobObj::CDobObj(void)
	:_buf(NULL)
{
	m_DobFile = new CVirtualFile();
	G_Guard Lock(m_Lock);
	if( !_buf){
		_buf = new char[buf_size_transfer];
	}
}


CDobObj::~CDobObj(void)
{
	G_Guard Lock(m_Lock);
	if( _buf )
	{
		delete[] _buf;
	}
	_buf = NULL;
}

CDobObj::CDobObj(std::string filename)
	:_buf(NULL)
{
	m_DobFile = new CVirtualFile(filename);
	G_Guard Lock(m_Lock);
	if( !_buf){
		_buf = new char[buf_size_transfer];
	}
}

CVirtualFilePtr CDobObj::vptr()
{
	return m_DobFile;
}

bool CDobObj::write(string dobfile,DOBPOS &pos)
{
	if( !_buf){
		return false;
	}

	FILE *fp = fopen(dobfile.c_str(),"rb");
	_fseeki64(fp,0,2);
	hfsuint64 flen = ftell(fp);
	_fseeki64(fp,0,0);

	pos.ulPos = m_DobFile->vptr();
	pos.ulSize = 0;
	pos.uiFileNo = 0;

	while( pos.ulSize < (hfsuint64)flen){
		INT64 t = fread(_buf,1,buf_size_transfer,fp);
		pos.ulSize +=t;
		_fseeki64(fp,pos.ulSize,0);
		INT64 w = m_DobFile->Write(_buf,1,t);
		if( w != t){
			return false;
		}
	}
	m_DobFile->Flush();
	fclose(fp);
	return true;
}

bool CDobObj::read(DOBPOS &pos)
{
	m_DobFile->Seek(pos.ulPos,0);
	hfsuint64 r = 0;
	while( r < pos.ulSize)
	{
		r += m_DobFile->Read(_buf,1,buf_size_transfer);
	}

	return true;
}

bool CDobObj::read(DOBPOS &pos,FILE *to)
{
	m_DobFile->Seek(pos.ulPos,0);
	hfsuint64 r = 0;
	while( r < pos.ulSize)
	{
		hfsint nCount = (hfsint)(pos.ulSize-r);
		nCount = min(nCount,buf_size_transfer);
		hfsint b = (hfsint)m_DobFile->Read(_buf,1,nCount);
		fwrite(_buf,1,b,to);
		r += b;
	}

	return true;
}

bool CDobObj::read(DOBPOS &pos,void *buff,hfsint& buflen)
{
	m_DobFile->Seek(pos.ulPos,0);
	hfsuint64 r = 0;
	hfsint nCount = (hfsint)min(pos.ulSize,buflen);
	r = m_DobFile->Read(buff,1,nCount);
	if( r == nCount){
		buflen = nCount;
		return true;
	}
	buflen = 0;
	return false;
}

bool CDobObj::read(DOBPOS &pos,hfsint start,void *buff,hfsint& buflen)
{
	m_DobFile->Seek(pos.ulPos+start,0);
	hfsuint64 r = 0;
	hfsint nCount = (hfsint)min(pos.ulSize,buflen);
	r = m_DobFile->Read(buff,1,nCount);
	if( r == nCount){
		buflen = nCount;
		return true;
	}
	buflen = 0;
	return false;
}