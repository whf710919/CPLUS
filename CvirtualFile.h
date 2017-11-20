
#if !defined(__CVirtualFile_h_)
#define __CVirtualFile_h_

#include "dbobj.h"
#include "DataDef.h"
#include "WinFile.h"

const hfsuint64 MAX_SINGLE_FILE =((hfsuint64)1024*\
	(hfsuint64)1024*(hfsuint64)1024*(hfsuint64)2-\
	(hfsuint64)1024);
#include "GLock.h"

class  CVirtualFile:public CRefCounter
{
public:
	CVirtualFile(void);
	CVirtualFile(hfsint64 maxsize);
	~CVirtualFile(void);
	CVirtualFile(hfststring sFileName);

	inline operator FILE*(void) const
	{
		return m_fp->ptr();
	}

	bool		Open(hfststring sFileName,hfststring opr);
	bool		Close();	
	hfsuint64	Seek(hfsuint64 Offset, int Origin);
	hfsbool		MoveNext();
	hfsuint64	Write(CDbObjPtr &o);
	hfsuint64	Write(const void *pStr, hfsuint64 nElemectSize, hfsuint64 nCount);
	hfsuint64	Read(void *pDstBuf, hfsint nElementSize, hfsint nCount);
	hfsuint64	FileSize();
	bool		Flush();
	hfsuint64	vptr();
	hfsuint64	flen();
	CFileDriverPtr fp();
protected:
	string		GetDiskName(int i);
	int			FindObj(CFileDriverPtr fp);
private:
	vector<CFileDriverPtr>	m_fps;
	hfsuint64		m_fpos;	
	string			m_FileName;
	CFileDriverPtr		m_fp;
	string			m_opr;
	hfsint64		m_MaxBlockSize;
	G_Lock			m_Lock;
};

typedef Ptr<CVirtualFile> CVirtualFilePtr;

#endif

