#pragma once

class CFolder
{
public:
	CFolder(void);
	~CFolder(void);

protected:
	enum OperateType{
			FOLDER_FILES_LIST,	//获得指定文件夹下的所有文件(带路径的文件)
			FOLDER_DEL_FILES,	//删除指定文件夹下的所有文件
	};

	struct SDirWalkData
	{
		OperateType		m_eOpType;
		BOOL			m_bRecurse;			//Set to TRUE to enter subdirectories.
		TCHAR			m_szFileName[_MAX_PATH];
		TCHAR			m_szFilePath[_MAX_PATH];
		BOOL			m_bOk;				//Loop control flag
		BOOL			m_bIsDir;			//Loop control flag
		WIN32_FIND_DATA m_FindData;
	};

	BOOL IsChildDir(WIN32_FIND_DATA *lpFindData);
	HANDLE FindFirstChildDir(LPTSTR szPath, WIN32_FIND_DATA *lpm_FindData);
	BOOL FindNextChildDir(HANDLE hFindFile, WIN32_FIND_DATA *lpm_FindData);

	BOOL DirWalkRecurse(SDirWalkData * pDW, char *pExt);
	BOOL MakeDir(const char *pszPath);
public:
	BOOL CopyHotstarDir(char *srcDir, char *desDir);
	BOOL CopyDir(char *srcDir, char *desDir, char *exclude=NULL);

	BOOL GetFilesList(char *pszPath, char *pExt, vector<string> &strFileArray);
	BOOL DelFiles(char *pszPath, char *pExt);
private:
	char			m_szCurPath[_MAX_PATH];
	vector<string>	m_FileArray;
	string			m_strTargetPath;
};
