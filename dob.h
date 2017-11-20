

#if !defined(__CDobObj_h_)
#define __CDobObj_h_
#include "CvirtualFile.h"
#include "glock.h"
#include "DataDef.h"

class  CDobObj:public CRefCounter
{
public:
	CDobObj(void);
	~CDobObj(void);
	CDobObj(string filename);
	bool write(string dobfile,DOBPOS &pos);
	bool read(DOBPOS &pos);
	bool read(DOBPOS &pos,FILE *to);
	bool read(DOBPOS &pos,void *buff,int& buflen);
	bool read(DOBPOS &pos,int start,void *buff,int& buflen);
private:
	G_Lock m_Lock;
public:
	CVirtualFilePtr m_DobFile;
	CVirtualFilePtr vptr();
	char *_buf;
};

typedef Ptr<CDobObj> CDobObjPtr;
#endif