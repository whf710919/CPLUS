#ifndef HFS_SERVER_TEMPFILE_H_
#define HFS_SERVER_TEMPFILE_H_
#include "DataDef.h"
#include "WinFile.h"

class CServerTempFile:public CFileDriver
{
public:
	CServerTempFile(void);
	~CServerTempFile(void);
	inline operator CFileDriverPtr()
	{
		return (CFileDriverPtr)this;
	}
};

typedef Ptr<CServerTempFile> CServerTempFilePtr;
#endif