// hfsystem.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"

#include <sys/stat.h>
#include "Server.h"
#include "DataDef.h"
#include "EappGet.h"
#include "Utility.h"
#include "WinFile.h"

#if defined(_WINDOWS_)
	#include <windows.h>
#endif

CRefCount *G_Service = 0;

hfsbool GetServiceName(pchfschar pCmdParam,phfschar pSName)
{
	if(!stricmp(pCmdParam, "-eap")){
		strcpy(pSName,EAP_SERVICE_NAME);
	}
	else{
		pSName[0]='\0';
	}
	if(pSName[0] != '\0'){
		return true;
	}
	return false;
}

hfsbool GetServiceType(pchfschar pCmdParam,hfsint &pSType)
{
	pSType = -1;

	if(_strnicmp(pCmdParam, "-i", 2) == 0){
		pSType = CRefCount::INSTALL;
	}
	else if(_strnicmp(pCmdParam, "-u", 2) == 0 ){
		pSType = CRefCount::REMOVE;
	}
	else if(_strnicmp(pCmdParam, "-s", 2) == 0){
		pSType = CRefCount::STARTUP;
	}
	else if(_strnicmp(pCmdParam, "-e", 2) == 0){
		pSType = CRefCount::END;
	}
	else if(_strnicmp(pCmdParam, "-p", 2) == 0){
		pSType = CRefCount::PAUSE;
	}
	else if(_strnicmp(pCmdParam, "-c", 2) == 0){
		pSType = CRefCount::CONTINUE;
	}
	else if(_strnicmp(pCmdParam, "-d", 2) == 0){
		pSType = CRefCount::MYDEBUG;
	}
	return true;
}

hfsbool ParseCmaLine(pchfschar lpCmdLine,phfschar pSName,phfschar pCmdLine,hfsint &pSType)
{
	hfsint	argc;
	vector<string> cmds;
	CUtility::Str()->CStrToStrings(lpCmdLine,cmds,' ');
	
	argc = cmds.size();
	
	if( argc < 1 ){
		return false;
	}
	else if( argc == 2 ){
		if( !GetServiceName(cmds[0].c_str(), pSName)){
			return false;
		}
		strcpy(pCmdLine,cmds[0].c_str());
		strcat(pCmdLine," ");
		strcat(pCmdLine, cmds[1].c_str());
	}
	else if( argc >= 3){
		if(!GetServiceType(cmds[0].c_str(),pSType)||
			!GetServiceName(cmds[1].c_str(),pSName)){
			return false;
		}
		strcpy(pCmdLine,cmds[1].c_str());
		strcat(pCmdLine," ");
		strcat(pCmdLine, cmds[2].c_str());
	}

	return true;
}

int APIENTRY _tWinMain(HINSTANCE hInstance,
	HINSTANCE hPrevInstance,
	LPTSTR    lpCmdLine,
	int       nCmdShow)
{
	hfsint	iServiceType=-1;
	hfschar	cServiceName[MAX_PATH];
	hfschar cCmdLine[MAX_PATH];
	string  sConfigHost;
	//hfsint	e;

	if(!ParseCmaLine(lpCmdLine,cServiceName,cCmdLine,iServiceType)){
		CUtility::Logger()->d("service no parameters.");
		return 1;
	}
	else{
		phfschar p = strstr(cCmdLine,"-h");
		if(!p){
			CUtility::Logger()->d("parameter no config host.");
			return 1;
		}
		//if( (e = CUtility::InitServer(p+2)) < _ERR_SUCCESS){
		//	CUtility::Logger()->d("init server error.e:%d",e);
		//}
		G_Service = new CRefCount(cServiceName,cServiceName);
		if(!G_Service){
			CUtility::Logger()->d("service memory error.");
			return 1;
		}
		G_Service->SetCmdLine(cCmdLine);
	}
	if( iServiceType == CRefCount::MYDEBUG){
		G_Service->Run(0,0);
		return 0;
	}
	return G_Service->RegisterService(iServiceType);
}
