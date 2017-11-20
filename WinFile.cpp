#include "StdAfx.h"
#include "WinFile.h"
#include "Utility.h"

CFile::CFile()
{
	CommonBaseInit(INVALID_HANDLE_VALUE);
}

CFile::CFile(HANDLE hFile)
{
	if(hFile != INVALID_HANDLE_VALUE){
		Close();
	}
	CommonBaseInit(hFile);
}

CFile::CFile(pchfschar lpszFileName, UINT nOpenFlags)
{
	CommonInit(lpszFileName, nOpenFlags);
}

void CFile::CommonBaseInit(HANDLE hFile)
{
	m_hFile = hFile;
	m_bCloseOnDelete = FALSE;
}

void CFile::CommonInit(LPCTSTR lpszFileName, UINT nOpenFlags)
{
	if(lpszFileName == NULL){
		return;
	}
	CommonBaseInit(INVALID_HANDLE_VALUE);

	if (!Open(lpszFileName, nOpenFlags))
	{
		return;
	}
}

CFile::~CFile()
{
	if (m_hFile != INVALID_HANDLE_VALUE && m_bCloseOnDelete){
		Close();
	}
}

CFile* CFile::Duplicate() const
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return NULL;
	}

	CFile* pFile = new CFile();
	HANDLE hFile;
	if (!::DuplicateHandle(::GetCurrentProcess(), m_hFile,
		::GetCurrentProcess(), &hFile, 0, FALSE, DUPLICATE_SAME_ACCESS))
	{
		delete pFile;
	}
	pFile->m_hFile = hFile;
	pFile->m_bCloseOnDelete = m_bCloseOnDelete;
	
	return pFile;
}

BOOL CFile::Open(pchfschar lpszFileName, UINT nOpenFlags)
{
	if(!lpszFileName || lpszFileName[0] == '\0'){
		return FALSE;
	}

	
	if(m_hFile != INVALID_HANDLE_VALUE){
		return FALSE;
	}

	
	m_bCloseOnDelete = FALSE;

	m_hFile = INVALID_HANDLE_VALUE;
	
	if(strlen(lpszFileName) > MAX_PATH){
		return FALSE;
	}

	DWORD dwAccess = 0;
	switch (nOpenFlags & 3)
	{
	case modeRead:
		dwAccess = GENERIC_READ;
		break;
	case modeWrite:
		dwAccess = GENERIC_WRITE;
		break;
	case modeReadWrite:
		dwAccess = GENERIC_READ | GENERIC_WRITE;
		break;
	default:
		return FALSE;
	}

	DWORD dwShareMode = 0;
	switch (nOpenFlags & 0x70)
	{
	default:
		return FALSE;
	case shareCompat:
	case shareExclusive:
		dwShareMode = 0;
		break;
	case shareDenyWrite:
		dwShareMode = FILE_SHARE_READ;
		break;
	case shareDenyRead:
		dwShareMode = FILE_SHARE_WRITE;
		break;
	case shareDenyNone:
		dwShareMode = FILE_SHARE_WRITE | FILE_SHARE_READ;
		break;
	}

	
	SECURITY_ATTRIBUTES sa;
	sa.nLength = sizeof(sa);
	sa.lpSecurityDescriptor = NULL;
	sa.bInheritHandle = (nOpenFlags & modeNoInherit) == 0;

	// map creation flags
	DWORD dwCreateFlag;
	if (nOpenFlags & modeCreate)
	{
		if (nOpenFlags & modeNoTruncate)
			dwCreateFlag = OPEN_ALWAYS;
		else
			dwCreateFlag = CREATE_ALWAYS;
	}
	else
		dwCreateFlag = OPEN_EXISTING;

	
	DWORD dwFlags = FILE_ATTRIBUTE_NORMAL;
	if (nOpenFlags & osNoBuffer)
		dwFlags |= FILE_FLAG_NO_BUFFERING;
	if (nOpenFlags & osWriteThrough)
		dwFlags |= FILE_FLAG_WRITE_THROUGH;
	if (nOpenFlags & osRandomAccess)
		dwFlags |= FILE_FLAG_RANDOM_ACCESS;
	if (nOpenFlags & osSequentialScan)
		dwFlags |= FILE_FLAG_SEQUENTIAL_SCAN;

	HANDLE hFile = 
	::CreateFile(lpszFileName, dwAccess, dwShareMode, &sa, dwCreateFlag, dwFlags, NULL);
	if (hFile == INVALID_HANDLE_VALUE){		
		return FALSE;
	}
	m_hFile = hFile;
	m_bCloseOnDelete = TRUE;

	return TRUE;
}

UINT CFile::Read(void* lpBuf, UINT nCount)
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return 0;
	}

	if (nCount == 0){
		return 0;
	}

	if(lpBuf == NULL){
		return 0;
	}

	DWORD dwRead;
	if (!::ReadFile(m_hFile, lpBuf, nCount, &dwRead, NULL)){
		return 0;
	}	
	return (UINT)dwRead;
}

UINT CFile::Write(const void* lpBuf, UINT nCount)
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return 0;
	}

	if (nCount == 0){
		return 0;
	}
	if(lpBuf == NULL){
		return 0;
	}

	DWORD nWritten;
	if (!::WriteFile(m_hFile, lpBuf, nCount, &nWritten, NULL)){
		return 0;
	}

	if (nWritten != nCount){
		return 0;
	}
	
	return nWritten;
}

ULONGLONG CFile::Seek(LONGLONG lOff, UINT nFrom)
{
	
	if(m_hFile == INVALID_HANDLE_VALUE){
		return 0;
	}
	if(nFrom != begin && nFrom != end && nFrom != current){
		return 0;
	}
	
	LARGE_INTEGER liOff;

	liOff.QuadPart = lOff;
	liOff.LowPart = ::SetFilePointer(m_hFile, liOff.LowPart, &liOff.HighPart,
		(DWORD)nFrom);
	if (liOff.LowPart  == (DWORD)-1){
		if (::GetLastError() != NO_ERROR){
			//CUtility::Logger()->trace("SetFilePointer(%s) error.",m_strFileName);
			return 0;
		}
	}

	return liOff.QuadPart;
}

ULONGLONG CFile::GetPosition() const
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return 0;
	}

	LARGE_INTEGER liPos;
	liPos.QuadPart = 0;
	liPos.LowPart = ::SetFilePointer(m_hFile, liPos.LowPart, &liPos.HighPart , FILE_CURRENT);
	if (liPos.LowPart == (DWORD)-1){
		if (::GetLastError() != NO_ERROR){
			//CUtility::Logger()->trace("SetFilePointer(%s) error.",m_strFileName);
			return 0;
		}
	}

	return liPos.QuadPart;
}

void CFile::Flush()
{
	if (m_hFile == INVALID_HANDLE_VALUE)
		return;

	FlushFileBuffers(m_hFile);
}

void CFile::Close()
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return;
	}
	if (m_hFile != INVALID_HANDLE_VALUE)
		if(!::CloseHandle(m_hFile)){
			//CUtility::Logger()->trace("ClodeHandle(%s) error.",m_strFileName);
		}

	m_hFile = INVALID_HANDLE_VALUE;
	m_bCloseOnDelete = FALSE;

}

void CFile::Abort()
{
	if (m_hFile != INVALID_HANDLE_VALUE)
	{
		::CloseHandle(m_hFile);
		m_hFile = INVALID_HANDLE_VALUE;
	}
}

void CFile::LockRange(ULONGLONG dwPos, ULONGLONG dwCount)
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return;
	}

	ULARGE_INTEGER liPos;
	ULARGE_INTEGER liCount;

	liPos.QuadPart = dwPos;
	liCount.QuadPart = dwCount;
	if (!::LockFile(m_hFile, liPos.LowPart, liPos.HighPart, liCount.LowPart, 
		liCount.HighPart))
	{
		//CUtility::Logger()->trace("LockRange() error.%d,%s",GetLastError(), m_strFileName);
	}
}

void CFile::UnlockRange(ULONGLONG dwPos, ULONGLONG dwCount)
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return;
	}

	ULARGE_INTEGER liPos;
	ULARGE_INTEGER liCount;

	liPos.QuadPart = dwPos;
	liCount.QuadPart = dwCount;
	if (!::UnlockFile(m_hFile, liPos.LowPart, liPos.HighPart, liCount.LowPart,
		liCount.HighPart))
	{
		//CUtility::Logger()->trace("UnlockRange() error.%d,%s",GetLastError(), m_strFileName);
	}
}

void CFile::SetLength(ULONGLONG dwNewLen)
{
	if(m_hFile == INVALID_HANDLE_VALUE){
		return;
	}

	Seek(dwNewLen, (UINT)begin);

	if (!::SetEndOfFile(m_hFile))
	{
		//CUtility::Logger()->trace("SetLength() error.%d,%s",GetLastError(), m_strFileName);
	}
}

ULONGLONG CFile::GetLength() const
{
	ULARGE_INTEGER liSize;
	liSize.LowPart = ::GetFileSize(m_hFile, &liSize.HighPart);
	if (liSize.LowPart == INVALID_FILE_SIZE){
		if (::GetLastError() != NO_ERROR)
		{
			//CUtility::Logger()->trace("GetLength() error.%d,%s",GetLastError(), m_strFileName);
		}

	}
	return liSize.QuadPart;
}

UINT CFile::GetBufferPtr(UINT nCommand, UINT /*nCount*/,
	void** /*ppBufStart*/, void** /*ppBufMax*/)
{
	return 0;   // no support
}

void PASCAL CFile::Rename(pchfschar lpszOldName, pchfschar lpszNewName)
{
	BOOL bRes = ::MoveFile((LPTSTR)lpszOldName, (LPTSTR)lpszNewName);
	if (!bRes){
		//CUtility::Logger()->trace("Rename() error.%d,%s",GetLastError(), lpszNewName);
	}
}

void PASCAL CFile::Remove(pchfschar lpszFileName)
{
	BOOL bRes = ::DeleteFile((LPTSTR)lpszFileName);
	if (!bRes){
		//CUtility::Logger()->trace("DeleteFile() error.%d,%s",GetLastError(), lpszFileName);
	}
}

hfsstring CFile::GetFileName()
{
	return m_strFileName;
}

CWinFile::CWinFile()
{
	fp = 0;
}

CWinFile::CWinFile(pchfschar pFilename)
{
	m_strFileName = pFilename;
	_filename = pFilename;
	fp = 0;
}

CWinFile::~CWinFile()
{
	Close();
}

bool CWinFile::fopen(pchfschar pFileName,const hfschar *mode)
{
	m_strFileName = pFileName;
	_mode = mode;
	_filename = pFileName;
	UINT flag = 0;
	if(iswrite()){
		if(!exist() || !isshare()){
			flag |= CFile::modeCreate;
		}
		if(isread()){
			flag |= CFile::modeReadWrite|CFile::shareDenyNone |CFile::typeBinary;
		}else{
			if(isshare()){
				flag |= CFile::modeWrite|CFile::shareDenyNone |CFile::typeBinary;
			}else{
				flag |= CFile::modeWrite|CFile::shareDenyWrite |CFile::typeBinary;
			}
		}
	}else{
		if(isread()){
			flag |= CFile::modeRead|CFile::shareDenyWrite|CFile::typeBinary;
		}else{
			return false;
		}
	}
	return Open(pFileName,flag) != 0 ? true:false;
}

bool CWinFile::fopen(const hfschar *mode)
{
	return fopen(m_strFileName.c_str(),mode);
}

hfssize	CWinFile::fread(void* buffer,hfsint size,hfsint count)
{
	return Read(buffer,size*count);
}

//hfssize CWinFile::fseek(hfsint64 offset,hfsint origin)
//{
//	if(origin == 0){
//		if(offset > GetLength()){
//			SetLength(offset);
//		}
//	}
//	return (hfsint)Seek(offset,origin);
//}

hfsint64 CWinFile::fseek64(hfsint64 offset,hfsint origin)
{
	if(origin == 0){
		if(offset > (hfsint64)GetLength()){
			SetLength(offset);
		}
	}
	return Seek(offset,origin);
}

bool CWinFile::feof()
{
	ULONGLONG size = GetLength();
	ULONGLONG pos = GetPosition();
	if(pos >= size){
		return true;
	}

	return false;
}

hfsint CWinFile::fflush()
{
	return 0;
}

hfsuint64 CWinFile::ftell()
{
	return GetPosition();
}

hfsuint64 CWinFile::ftell64()
{
	return GetPosition();
}

hfssize CWinFile::fwrite(const void* pBuffer,hfsint element,hfsint size,hfsint64 pos)
{
	G_Guard Lock(m_Lock);
	
	if(pos >= 0 ){
		fseek64(pos,0);
	}
	
	return Write(pBuffer,element*size);
}

void CWinFile::fclose()
{
	Close();
}

hfsint CWinFile::remove()
{
	Close();
	Remove(m_strFileName.c_str());
	return 0;
}

hfsbool CWinFile::copy(pchfschar newfile,void *pbuff,hfsint size)
{
	Rename(m_strFileName.c_str(),newfile);
	return true;
}

hfsbool CWinFile::iswrite()
{
	return CHFSFile::iswrite();
}

hfsstring CWinFile::GetFile()
{
	return m_strFileName;
}

hfsbool CWinFile::exist()
{
	return CHFSFile::exist();
}