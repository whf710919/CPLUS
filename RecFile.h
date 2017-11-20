#ifndef HFS_RECFILE_H_
#define  HFS_RECFILE_H_

#include "DataDef.h"
#include "smartptr.h"
#include "WinFile.h"
#include "Utility.h"
#include "Error.h"
#include "ServerTempFile.h"

class CRecFile:public CRefCounter
{
public:
	CRecFile(hfststring tsTempName);
	~CRecFile(void);
	hfststring GetFile();
	hfsint Write();	
	hfsint Write(hfsuint64 pos);
	hfsstring RecText(hfsuint64 pos);
	hfststring  GetRecName();
	hfststring	GetSQL(hfsstring table,hfsuint64 pos);
	hfsint WriteRef(hfsint64 id, hfsstring &sResult, hfsint iRecCount, hfsint iCiteCount, hfsint iCitedCount);	
protected:
	hfststring GetTableName();
	hfststring GetDirName();

	hfststring	GetFileName();
	hfststring GetFileExt();
	hfststring GetCreateDate();
	hfststring GetModifyDate();
	hfststring	GetCRC32();
	hfsstring	GetMD5();
	hfststring	GetFileSize(CFileDriverPtr fp);
	hfsint		WriteItem (CFileDriverPtr fp, hfststring Name, hfststring Value, hfsbool bNewLine,hfsbool bDob=false);
	hfsint		WriteItem(CFileDriverPtr fp, hfststring Name, int Value, hfsbool bNewLine,hfsbool bDob = false);
private:
	CServerTempFilePtr		rec;
	CFileDriverPtr		   dob;
};

typedef Ptr<CRecFile> CRecFilePtr;
#endif