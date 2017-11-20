#ifndef HFS_WIN_FILE_H_
#define HFS_WIN_FILE_H_
#include "smartptr.h"
#include "DataDef.h"
#include "hfsfile.h"
#include "GLock.h"

class CFile
{
	
public:
	// Flag values
	enum OpenFlags {
		modeRead =         (int) 0x00000,
		modeWrite =        (int) 0x00001,
		modeReadWrite =    (int) 0x00002,
		shareCompat =      (int) 0x00000,
		shareExclusive =   (int) 0x00010,
		shareDenyWrite =   (int) 0x00020,
		shareDenyRead =    (int) 0x00030,
		shareDenyNone =    (int) 0x00040,
		modeNoInherit =    (int) 0x00080,
		modeCreate =       (int) 0x01000,
		modeNoTruncate =   (int) 0x02000,
		typeText =         (int) 0x04000, // typeText and typeBinary are
		typeBinary =       (int) 0x08000, // used in derived classes only
		osNoBuffer =       (int) 0x10000,
		osWriteThrough =   (int) 0x20000,
		osRandomAccess =   (int) 0x40000,
		osSequentialScan = (int) 0x80000,
	};

	enum Attribute {
		normal =    0x00,
		readOnly =  0x01,
		hidden =    0x02,
		system =    0x04,
		volume =    0x08,
		directory = 0x10,
		archive =   0x20
	};

	enum SeekPosition { begin = 0x0, current = 0x1, end = 0x2 };

	CFile();

	CFile(HANDLE hFile);
	CFile(pchfschar lpszFileName, UINT nOpenFlags);

	HANDLE m_hFile;
	operator HANDLE() const;

	virtual ULONGLONG GetPosition() const;
	
	virtual hfsstring GetFileName();
	virtual BOOL Open(LPCTSTR lpszFileName, UINT nOpenFlags);
	static void PASCAL Rename(pchfschar lpszOldName, pchfschar lpszNewName);
	static void PASCAL Remove(pchfschar lpszFileName); 
	ULONGLONG SeekToEnd();
	void SeekToBegin();
	virtual CFile* Duplicate() const;
	virtual ULONGLONG Seek(LONGLONG lOff, UINT nFrom);
	virtual void SetLength(ULONGLONG dwNewLen);
	virtual ULONGLONG GetLength() const;

	virtual UINT Read(void* lpBuf, UINT nCount);
	virtual UINT Write(const void* lpBuf, UINT nCount);

	virtual void LockRange(ULONGLONG dwPos, ULONGLONG dwCount);
	virtual void UnlockRange(ULONGLONG dwPos, ULONGLONG dwCount);

	virtual void Abort();
	virtual void Flush();
	virtual void Close();

public:
	virtual ~CFile();
	enum BufferCommand { bufferRead, bufferWrite, bufferCommit, bufferCheck };
	enum BufferFlags 
	{ 
		bufferDirect = 0x01,
		bufferBlocking = 0x02
	};
	virtual UINT GetBufferPtr(UINT nCommand, UINT nCount = 0,
		void** ppBufStart = NULL, void** ppBufMax = NULL);

protected:
	void CommonBaseInit(HANDLE hFile);
	void CommonInit(pchfschar lpszFileName, UINT nOpenFlags);

	BOOL m_bCloseOnDelete;
	hfsstring m_strFileName;
};

class CWinFile:public CFile,public CHFSFile
{
public:
	CWinFile();
	CWinFile(pchfschar pFilename);
	~CWinFile();
	virtual bool fopen(pchfschar pFileName,const hfschar *mode);
	virtual bool fopen(const hfschar *mode);
	virtual hfssize	fread(void* buffer,hfsint size,hfsint count);
	//virtual hfsint64 fseek(hfsint64 offset,hfsint origin);
	virtual hfsint64 fseek64(hfsint64 offset,hfsint origin);
	virtual bool feof();
	virtual hfsint fflush();
	virtual hfsuint64 ftell();
	virtual hfsuint64 ftell64();
	virtual hfssize fwrite(const void* pBuffer,hfsint element,hfsint size,hfsint64 pos=-1);
	virtual void fclose();
	virtual hfsint remove();
	virtual hfsbool copy(pchfschar newfile,void *pbuff,hfsint size);
	virtual hfsbool iswrite();
	virtual hfsstring GetFile();
	virtual hfsbool exist();
private:
	G_Lock m_Lock;
};

#ifdef _WINDOWS_
class CFileDriver:public CWinFile
#else
class CFileDriver;public CHFSFile
#endif
{
public:
	CFileDriver()
	{

	}
	CFileDriver(pchfschar pFilename)
		:CWinFile(pFilename)
	{

	}
	CFileDriver(hfsstring sFilename)
		:CWinFile(sFilename.c_str())
	{

	}
	~CFileDriver()
	{

	}
};

typedef Ptr<CFileDriver> CFileDriverPtr;

#endif