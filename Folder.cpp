#include "StdAfx.h"
#include "Folder.h"
extern bool WriteLog(const char *str);

CFolder::CFolder(void)
{
	m_FileArray.clear();
	m_strTargetPath.clear();

	memset( m_szCurPath, 0, sizeof(m_szCurPath) );
}

CFolder::~CFolder(void)
{
}

BOOL CFolder::IsChildDir(WIN32_FIND_DATA *lpm_FindData)
{
	return(
		( (lpm_FindData->dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0)	&&
		  (lstrcmp(lpm_FindData->cFileName, _TEXT(".") ) != 0)				&&
		  (lstrcmp(lpm_FindData->cFileName, _TEXT("..") ) != 0) );

}

BOOL CFolder::FindNextChildDir(HANDLE hFindFile, WIN32_FIND_DATA *lpm_FindData)
{
	BOOL bFound = FALSE;
	do{
		bFound = FindNextFile(hFindFile, lpm_FindData);
	}while(bFound && !IsChildDir(lpm_FindData) );

	return (bFound);

}

HANDLE CFolder::FindFirstChildDir(LPTSTR szPath, WIN32_FIND_DATA *lpm_FindData)
{
	BOOL bFound;
	HANDLE hFindFile = FindFirstFile(szPath, lpm_FindData);

	if(hFindFile != INVALID_HANDLE_VALUE)
	{
		bFound = IsChildDir(lpm_FindData);

		if(!bFound)
		{
			bFound = FindNextChildDir(hFindFile, lpm_FindData);
		}

		if(!bFound)
		{
			FindClose(hFindFile);
			hFindFile = INVALID_HANDLE_VALUE;
		}
	}

	return(hFindFile);
}

BOOL CFolder::DirWalkRecurse(SDirWalkData *pDW, char *pExt)
{
	if( !pDW )
		return FALSE;

	GetCurrentDirectory( sizeof(pDW->m_szFilePath), pDW->m_szFilePath);
	HANDLE hFind = FindFirstFile(pExt, &pDW->m_FindData);
	pDW->m_bOk = (hFind != INVALID_HANDLE_VALUE);

	string str;
	while(pDW->m_bOk)
	{
		pDW->m_bIsDir = pDW->m_FindData.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY;

		if(!pDW->m_bIsDir || (! pDW->m_bRecurse && IsChildDir(&pDW->m_FindData) ) )
		{
			if( pDW->m_eOpType == FOLDER_FILES_LIST )
			{
				str = m_strTargetPath;
				str += "\\";
				str += pDW->m_FindData.cFileName;
				m_FileArray.push_back( str );
			}
			else if( pDW->m_eOpType == FOLDER_DEL_FILES )
			{
				if( !DeleteFile( pDW->m_FindData.cFileName ) )
				{
					return FALSE;
				}
			}
		}
		
		pDW->m_bOk = FindNextFile(hFind, &pDW->m_FindData);
	}

	if(hFind != INVALID_HANDLE_VALUE)
		FindClose(hFind);

	if(pDW->m_bRecurse)
	{
		hFind = FindFirstChildDir("*.*", &pDW->m_FindData);
		pDW->m_bOk = (hFind != INVALID_HANDLE_VALUE);

		while(pDW->m_bOk)
		{
			//change into the child directory.
			if(SetCurrentDirectory(pDW->m_FindData.cFileName) )
			{
				str = m_strTargetPath;
				m_strTargetPath += "\\";
				m_strTargetPath += pDW->m_FindData.cFileName;
		
				//Perform the recursive walk into the child
				//directory. Remember that some members of pDW
				//will be overwritten by this call.
				DirWalkRecurse(pDW, pExt);
				//Change back to the child's parent directory.
				SetCurrentDirectory("..");
				m_strTargetPath = str;
			}

			pDW->m_bOk  = FindNextChildDir(hFind, &pDW->m_FindData);
		}

		if(hFind != INVALID_HANDLE_VALUE)
			FindClose(hFind);
	}

	return TRUE;
}

////////////////////////////////////////////////////////////

BOOL CFolder::GetFilesList(char *pszPath, char *pExt, vector<string> &strFileArray)
{
	if( !pszPath || !strlen(pszPath) )
		return FALSE;

	strFileArray.clear();

	memset( m_szCurPath, 0, sizeof(m_szCurPath) );
	GetCurrentDirectory( sizeof(m_szCurPath), m_szCurPath );

	m_strTargetPath = pszPath;

	if( SetCurrentDirectory(pszPath) )
	{
		SDirWalkData DW;
		memset( &DW, 0, sizeof(SDirWalkData) );
		DW.m_bRecurse = TRUE;
		DW.m_eOpType = FOLDER_FILES_LIST;

		if( !DirWalkRecurse(&DW, pExt) )
		{
			SetCurrentDirectory(m_szCurPath);
			return FALSE;
		}
	}
	else
	{
		SetCurrentDirectory(m_szCurPath);
		return FALSE;
	}

	strFileArray.insert( strFileArray.begin(), m_FileArray.begin(), m_FileArray.end() );

	SetCurrentDirectory( m_szCurPath );

	return TRUE;
}

BOOL CFolder::CopyHotstarDir(char *srcDir, char *desDir)
{
	BOOL bRet = FALSE;
	
	char oldFile[MAX_PATH], oldDir[MAX_PATH];
	strcpy(oldDir, srcDir);
	if(oldDir[strlen(oldDir)-1] != '\\')
		strcat(oldDir, "\\");
	strcpy(oldFile, oldDir);
	strcat(oldFile, "*.*");
	
	WIN32_FIND_DATA finddata;
	HANDLE hfile = FindFirstFile(oldFile,&finddata);
	char newFile[MAX_PATH];
	while( hfile != INVALID_HANDLE_VALUE )
	{
		if ((finddata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0)  // if not directory
		{
			if( strstr(strlwr(finddata.cFileName), "hotstar_sys_field" ) != NULL || strstr(strlwr(finddata.cFileName), "hotstar_sys" ) != NULL ||
				strstr(strlwr(finddata.cFileName), "sys_db" ) != NULL )
			{
				strcpy(oldFile, oldDir);
				strcat(oldFile, finddata.cFileName);
				
				memset(newFile,0,sizeof(newFile));
				strcpy(newFile, desDir);
				if(newFile[strlen(newFile)-1] != '\\')
					strcat(newFile, "\\");
				strcat(newFile, finddata.cFileName);
				
				DWORD dwAttrs = GetFileAttributes( newFile ); 
				dwAttrs &= ~FILE_ATTRIBUTE_READONLY;
				SetFileAttributes(newFile, dwAttrs );
				if( !CopyFile(oldFile, newFile, FALSE) )
				{
					char szMsg[512];
					sprintf( szMsg, "Copy File %s to %s failed.", oldFile, newFile );
					//WriteLog( szMsg );
				}
			}
		}
		else if(strcmp(finddata.cFileName, ".") &&
			strcmp(finddata.cFileName, ".."))//它有子目录
		{
			strcpy(oldFile, oldDir);
			strcat(oldFile, finddata.cFileName);
			
			memset(newFile,0,sizeof(newFile));
			strcpy(newFile, desDir);
			if(newFile[strlen(newFile)-1] != '\\')
				strcat(newFile, "\\");
			strcat(newFile, finddata.cFileName);
			MakeDir(newFile);
			
			CopyHotstarDir(oldFile, newFile);
		}
		
		if( !FindNextFile(hfile,&finddata) )
			break;		   
	}
	
	if(  hfile != INVALID_HANDLE_VALUE )
		FindClose(hfile);
	
	return TRUE;
}

BOOL CFolder::CopyDir(char *srcDir, char *desDir, char *exclude)
{
	BOOL bRet = FALSE;
	
	char oldFile[MAX_PATH], oldDir[MAX_PATH];
	strcpy(oldDir, srcDir);
	if(oldDir[strlen(oldDir)-1] != '\\')
		strcat(oldDir, "\\");
	strcpy(oldFile, oldDir);
	strcat(oldFile, "*.*");
	
	char szKeyWord[MAX_PATH];
	szKeyWord[0] = '\0';
	if( exclude )
	{
		strcpy( szKeyWord, exclude );
		_strlwr( szKeyWord );
	}

	WIN32_FIND_DATA finddata;
	HANDLE hfile = FindFirstFile(oldFile,&finddata);
	char newFile[MAX_PATH];
	while( hfile != INVALID_HANDLE_VALUE )
	{
		if ((finddata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0)  // if not directory
		{
			if( szKeyWord[0] == '\0' || strstr(strlwr(finddata.cFileName), szKeyWord ) == NULL )
			{
				strcpy(oldFile, oldDir);
				strcat(oldFile, finddata.cFileName);
				
				memset(newFile,0,sizeof(newFile));
				strcpy(newFile, desDir);
				if(newFile[strlen(newFile)-1] != '\\')
					strcat(newFile, "\\");
				strcat(newFile, finddata.cFileName);
				
				CopyFile(oldFile, newFile, FALSE);	
			}
		}
		else if(strcmp(finddata.cFileName, ".") &&
			strcmp(finddata.cFileName, ".."))//它有子目录
		{
			strcpy(oldFile, oldDir);
			strcat(oldFile, finddata.cFileName);
			
			
			memset(newFile,0,sizeof(newFile));
			strcpy(newFile, desDir);
			if(newFile[strlen(newFile)-1] != '\\')
				strcat(newFile, "\\");
			strcat(newFile, finddata.cFileName);
			MakeDir(newFile);
			
			CopyDir(oldFile, newFile);
		}
		
		if( !FindNextFile(hfile,&finddata) )
			break;		   
	}
	
	if(  hfile != INVALID_HANDLE_VALUE )
		FindClose(hfile);
	
	return TRUE;
}

BOOL CFolder::DelFiles(char *pszPath, char *pExt)
{
	if( !pszPath || !strlen(pszPath) )
		return FALSE;

	memset( m_szCurPath, 0, sizeof(m_szCurPath) );
	GetCurrentDirectory( sizeof(m_szCurPath), m_szCurPath );

	m_strTargetPath = pszPath;

	if( SetCurrentDirectory(pszPath) )
	{
		SDirWalkData DW;
		memset( &DW, 0, sizeof(SDirWalkData) );
		DW.m_bRecurse = TRUE;
		DW.m_eOpType = FOLDER_DEL_FILES;

		if( !DirWalkRecurse(&DW, pExt) )
		{
			SetCurrentDirectory(m_szCurPath);
			return FALSE;
		}
	}
	else
	{
		SetCurrentDirectory(m_szCurPath);
		return FALSE;
	}

	SetCurrentDirectory( m_szCurPath );

	return TRUE;
}

BOOL CFolder::MakeDir(const char *pszPath)
{
	char DstBuf[MAX_PATH];
	char Buf[MAX_PATH];
	
	strcpy(DstBuf,pszPath);
	if( DstBuf[strlen(DstBuf)-1]=='\\')
		DstBuf[strlen(DstBuf)-1]=0;

	WIN32_FIND_DATA data;
	HANDLE handle=FindFirstFile(DstBuf,&data);
	if (handle!=INVALID_HANDLE_VALUE)
	{
		//the path is find
		FindClose(handle);
		return 0;
	}
	//began to create the new path
	BOOL bSucceed=false;
	int i=0;
	do
	{
		Buf[i]=DstBuf[i];
		if (Buf[i]=='\\'||Buf[i]==0)
		{
			Buf[i]=0;
			handle=FindFirstFile(Buf,&data);
			if (handle==INVALID_HANDLE_VALUE)
				bSucceed=CreateDirectory(Buf,NULL);

			Buf[i]=DstBuf[i];
			FindClose(handle);
		}
		i++;
	} while (Buf[i-1]!=0);

	if (!bSucceed)
		return FALSE;
	
	return TRUE;
}
