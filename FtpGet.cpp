/*
		Big thanks to  Ken Sutherland, he answered a post in
		codeguru and this class was made using his answer.
		Qu: How to Get file form ftp server
http://www.codeguru.com/bbs/wt/showpost.pl?Board=vc&Number=48384&page=&view=&sb=
*/

/*
		Copyright	Robert Lascelle
					RLProgrammation
					vlad3@sympatico.ca

You can use this source for anything, if any change, please show
me the change and explain them.

			It's my first i/o class over internet.
*/

// FtpGet.cpp: implementation of the CFtpGet class.

#include "stdafx.h"
// from my program
// #include "http.h"
//
#include "FtpGet.h"

#ifdef _DEBUG
#undef THIS_FILE
static char THIS_FILE[]=__FILE__;
#define new DEBUG_NEW
#endif

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

CFtpGet::CFtpGet()
{
	// get the name of the app
	strAppName.LoadString(AFX_IDS_APP_TITLE);

	// create an internet session
	pInternetSession = new CInternetSession(strAppName,
		INTERNET_OPEN_TYPE_PRECONFIG);

	// if Not good, show message + return
	// should never failed anyway
	if(!pInternetSession)
	{
		AfxMessageBox("Can't start internet session");
		return;
	}

}

CFtpGet::~CFtpGet()
{
	// close the internet session
	pInternetSession->Close();
	// delete the session
	if(pInternetSession != NULL)
		delete pInternetSession;

}

// function, in logical order

bool CFtpGet::SetAccessRight(CString userName,
							 CString userPass)
{
	// simply get username and password
	strPass = userPass;
	strUser = userName;
	if( (strPass == "") || (strUser == ""))
		return 0;

	return 1;
}

bool CFtpGet::OpenConnection(CString server)
{
	if(server == "")
		return 0;

	// put the server name in the CFtpGet class
	strServerName = server;

	try {
		// try to connect to a ftp server
		pFtpConnection = pInternetSession->GetFtpConnection(strServerName,
			strUser,
			strPass);
	} catch (CInternetException* pEx) 
	{
		// if failed, just show the error

		// Oops! We failed to connect!
		TCHAR szErr[1024];
		pEx->GetErrorMessage(szErr, 1024);
		TRACE(szErr);
		AfxMessageBox(szErr);
		pEx->Delete();
		return 0;// return 1 but previous error box have been showed
	}


	return 1;
}

bool CFtpGet::GetFile(CString remoteFile,
					  CString localFile)
{
	// Try to get the file
	BOOL bGotFile = pFtpConnection->GetFile(remoteFile,
		localFile,
		FALSE,
		FILE_ATTRIBUTE_NORMAL,	
		FTP_TRANSFER_TYPE_BINARY | INTERNET_FLAG_RELOAD | INTERNET_FLAG_NO_CACHE_WRITE);

	return bGotFile ? 1 : 0 ;
	// if bGotFile is 0 ( FALSE ), return 0
	// if bGotFile is 1 ( TRUE  ), return 1
}

int CFtpGet::GetMultipleFile(CStringArray *remoteArray,
							 CStringArray *localArray,
							 int number_file)
{
	// init some var
	BOOL goodfile;
	int x=0;
	int nb_lost_file =0;

	// while loop to transfer every file in the array
	while(x<number_file)
	{
		// try to get file
		goodfile = pFtpConnection->GetFile(remoteArray->GetAt(x),
			localArray->GetAt(x),
			FALSE,
			FILE_ATTRIBUTE_NORMAL,
			FTP_TRANSFER_TYPE_BINARY | INTERNET_FLAG_RELOAD | INTERNET_FLAG_NO_CACHE_WRITE);

		missed[x] = goodfile ? 0 : 1;
		// if failed, missed[x] become 1
		// if good, missed become 0
		if(missed[x])
			nb_lost_file++;
		// if the file was missed, increase the number of 
		// missing file.
		// increase to the next file
		x++;
	}
	//return the number of missing file, if any.
	return nb_lost_file;
}

bool CFtpGet::CloseConnection()
{
	// close the connection to server, you can reconnect latter
	if(pFtpConnection == NULL)
		return 0;
	try{
		pFtpConnection->Close();
	}catch(...)
	{
		return 0;
	}
	if(pFtpConnection != NULL)
		delete pFtpConnection;

	return 1;
}