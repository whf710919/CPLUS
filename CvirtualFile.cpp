#include "stdafx.h"
#include "CVirtualFile.h"
#include <io.h>
#include "Utility.h"

#define MAX_DOB_COUNT (10240)

CVirtualFile::CVirtualFile(void)
	:m_fpos(0)
	,m_FileName("")
	,m_fp(0)
	,m_MaxBlockSize(MAX_SINGLE_FILE)
{
	m_fps.clear();
}

CVirtualFile::CVirtualFile(hfsint64 maxsize)
	:m_fpos(0)
	,m_FileName("")
	,m_fp(0)
	,m_MaxBlockSize(maxsize)
{
	m_fps.clear();
}

CVirtualFile::~CVirtualFile(void)
{
	/*vector<CFileDriverPtr>::iterator it;
	for( it = m_fps.begin();it != m_fps.end();++it)
	{
	delete (*it);
	}*/
	m_fps.clear();
}


CVirtualFile::CVirtualFile(hfststring sFileName)
	:m_fpos(0)
	,m_FileName(sFileName)
	,m_fp(0)
	,m_MaxBlockSize(MAX_SINGLE_FILE)
{
	m_fps.clear();
}

bool CVirtualFile::Open(hfststring sFileName,hfststring opr)
{
	m_FileName = sFileName;
	m_opr = opr;
	int i = 0;
	int tcnt = 0;

	while( i < MAX_DOB_COUNT){
		if(access(GetDiskName(i).c_str(),0) == 0){
			tcnt = i;
		}
		i++;
	}

	i = 0;
	while(i <= tcnt){
		if(access(GetDiskName(i).c_str(),0) == 0){
			CFileDriverPtr fp = new CFileDriver();
			if( fp){
				if(fp->fopen(GetDiskName(i).c_str(),opr.c_str())){
					m_fps.push_back(fp);
				}else{
					//CUtility::Logger()->trace("FATIL ERROR open(%s) error.",GetDiskName(i).c_str());
					m_fps.push_back(0);
				}
			}else{
				//CUtility::Logger()->trace("FATIL ERROR new CFileDriverPtr(%s) error.",GetDiskName(i).c_str());
				m_fps.push_back(0);
			}
		}else{
			if(!CUtility::IsWrite(opr.c_str())){
				//CUtility::Logger()->trace("FATIL ERROR Find File(%s) error.",GetDiskName(i).c_str());
				m_fps.push_back(0);
			}
		}

		i++;
	}

	if( CUtility::IsWrite(opr.c_str()))
	{
		if(access(GetDiskName(0).c_str(),0) != 0){
			CFileDriverPtr fp = new CFileDriver();
			if( fp && fp->fopen(GetDiskName(0).c_str(),"wb")){
				fp->fclose();
				if(!fp->fopen(GetDiskName(0).c_str(),opr.c_str())){
					return false;
				}
				m_fps.push_back(fp);
			}else{
				return false;
			}
		}
	}

	if( !m_fps.empty())
	{
		m_fp = *m_fps.begin();
		Seek(0,0);
	}

	return true;
}


bool CVirtualFile::Close()
{
	G_Guard Lock(m_Lock);
	vector<CFileDriverPtr>::iterator it;
	for(it = m_fps.begin(); it != m_fps.end(); ++it){
		(*it)->fclose();
	}

	m_fps.clear();
	m_fp = (CFileDriver*)0;

	return true;
}

int CVirtualFile::FindObj(CFileDriverPtr fp)
{
	G_Guard Lock(m_Lock);
	for( hfsint i =0; i < (hfsint)m_fps.size();i++)
	{
		if(  m_fps[i] == fp ){
			return i;
		}
	}
	return -1;
}

hfsbool CVirtualFile::MoveNext()
{
	G_Guard Lock(m_Lock);
	int pos = FindObj(m_fp);
	pos++;
	if(pos >= m_fps.size()){
		string s = GetDiskName(pos);
		CFileDriverPtr fp = new CFileDriver();
		if( !fp || !fp->fopen(s.c_str(),m_opr.c_str())){
			return false;
		}
		m_fp = fp;
		m_fps.push_back(fp);
	}else{
		m_fp = m_fps[pos];
	}

	if(m_fp){
		m_fp->fseek64(0,SEEK_SET);
	}
	
	return true;
}

hfsuint64 CVirtualFile::Seek(hfsuint64 Offset, int Origin)
{
	G_Guard Lock(m_Lock);
	hfsuint64 vPos = 0;
	if(Origin == 0 ){
		vPos = Offset;
	}else if( Origin == 1 ){
		int pos = FindObj(m_fp);
		if(pos < 0 ){
			return -1;
		}
		vPos = (hfsuint64)pos * (m_MaxBlockSize);
		vPos += (hfsuint64)m_fp->ftell64();
		vPos += (hfsuint64)Offset;
	}else if( Origin == 2 ){
		vPos = FileSize()+Offset;
	}else{
		return -1;
	}

	int i = (int)( (vPos / m_MaxBlockSize));
	hfsuint64 m = vPos % m_MaxBlockSize;
	if( i < (int)m_fps.size()){
		m_fp = m_fps[i];
	}else{
		string s = GetDiskName(i);
		CFileDriverPtr fp = new CFileDriver();
		if( !fp || !fp->fopen(s.c_str(),m_opr.c_str())){
			return -1;
		}
		m_fp = fp;
		m_fps.push_back(fp);
	}
	if( m_fp ){
		return m_fp->fseek64(m,SEEK_SET);
	}
	return -1;
}

string CVirtualFile::GetDiskName(int i)
{
	char buf[MAX_PATH];
	const char *p = strrchr(m_FileName.c_str(),'.');
	if(p){
		strncpy(buf,m_FileName.c_str(),p-m_FileName.c_str());
		buf[p-m_FileName.c_str()] = '\0';
		sprintf(buf+(p-m_FileName.c_str()),"_%d%s",i+1,p);
	}else{
		strcpy(buf,m_FileName.c_str());
		sprintf(buf+m_FileName.size(),"_%d",i+1);
	}
	
	return (string)buf;
}

hfsuint64	CVirtualFile::Write(CDbObjPtr &o)
{
	return Write(o->GetData(),1,o->GetSize());
}

hfsuint64 CVirtualFile::Write(const void *pStr, hfsuint64 nElemectSize, hfsuint64 nCount)
{
	G_Guard Lock(m_Lock);
	if( !m_fp ){
		return -1;
	}
	hfsuint64 block = nElemectSize*nCount;
	if( block == 0 ){
		return 0;
	}

	hfsuint64 lpos = m_fp->ftell64();
	hfsuint w = (hfsuint)min((m_MaxBlockSize - lpos),block);
	hfsuint64 t = 0;

	if( w > 0 ){
		w = (hfsuint)m_fp->fwrite(pStr,1,(hfssize)w);
		if( w != min((m_MaxBlockSize - lpos),block) ){
			return -1;
		}
	}
	t = block - w;
	if( t > 0 ){
		if(!MoveNext()){
			return 0;
		}
		w += (hfsuint)Write((char*)pStr+w,1,t);
	}

	return w;
}

hfsuint64 CVirtualFile::Read(void *pDstBuf, hfsint nElementSize, hfsint nCount)
{
	if( !m_fp ){
		return -1;
	}
	hfsuint64 block = nElementSize*nCount;
	hfsuint64 lpos = m_fp->ftell64();
	hfsuint r = (hfsuint)min( (m_MaxBlockSize-lpos),block);

	if(  r > 0 ){
		r = (hfsuint)m_fp->fread(pDstBuf,1,(hfssize)r);
		if(r==0){
			return 0;
		}
		if( r != min((m_MaxBlockSize - lpos),block))
		{
			m_fp->fseek64(0,2);
			hfsuint64 len = m_fp->ftell64();
			m_fp->fseek64((hfsuint64)lpos,0);
			if( (hfsint64)len < m_MaxBlockSize){
				r = (hfsuint)(len - lpos);
				return Read(pDstBuf,1,r);
			}
			return -1;
		}
	}
	hfsuint64 t = block - r;
	if( t > 0 ){
		if(!MoveNext()){
			return 0;
		}
		r += (hfsuint)Read((char*)pDstBuf+r,1,(hfsuint)t);
	}
	return r;
}

hfsuint64 CVirtualFile::FileSize()
{
	if( !m_fps.empty()){
		CFileDriverPtr fp = m_fps[m_fps.size()-1];
		hfsuint64 c = fp->ftell64();
		fp->fseek64(0,2);
		hfsuint64 s = fp->ftell64();
		fp->fseek64(c,0);
		s += (m_MaxBlockSize*(m_fps.size()-1));
		return s;
	}
	return 0;
}

bool CVirtualFile::Flush()
{
	vector<CFileDriverPtr>::iterator it;
	for( it = m_fps.begin();it!= m_fps.end();++it)
	{
		if( (*it)->fflush() != 0 ){
			return false;
		}
	}
	return true;
}

hfsuint64 CVirtualFile::vptr()
{
	hfsuint64 vPos = 0;
	int pos = FindObj(m_fp);
	vPos = pos * (m_MaxBlockSize);
	vPos += m_fp->ftell64();

	return vPos;
}

hfsuint64 CVirtualFile::flen()
{
	int fh = _fileno(m_fp->ptr());
	return _filelength(fh);
}

CFileDriverPtr CVirtualFile::fp()
{
	return m_fp;
}