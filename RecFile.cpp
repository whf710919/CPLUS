#include "StdAfx.h"
#include "RecFile.h"
#include "CRC32.h"
#include "SmartBuffer.h"
#include "Utility.h"

CRecFile::CRecFile(hfststring tsTempName)
{
	dob = new CFileDriver(tsTempName);
}


CRecFile::~CRecFile(void)
{

}

hfsint CRecFile::Write()
{
	rec = new CServerTempFile();
	if( !rec->fopen(GetRecName().c_str(),"w")){
		return _ERR_FILE_OPEN;
	}
	if(! dob->fopen("rb")){
		return _ERR_FILE_OPEN;
	}

	WriteItem((CFileDriverPtr)rec,"FILENAME",GetFileName(),true);
	WriteItem((CFileDriverPtr)rec,"FILETYPE",GetFileExt(),false);
	WriteItem((CFileDriverPtr)rec,"FILESIZE",GetFileSize(dob),false);
	WriteItem((CFileDriverPtr)rec,"DIRNAME",GetDirName(),false);
	WriteItem((CFileDriverPtr)rec,"CREATEDATE",GetCreateDate(),false);
	WriteItem((CFileDriverPtr)rec,"MODIFYDATE",GetModifyDate(),false);
	WriteItem((CFileDriverPtr)rec,"MD5",GetMD5(),false);
	WriteItem((CFileDriverPtr)rec,"FLAG","0",false);
	WriteItem((CFileDriverPtr)rec,"NODEID",CUtility::Config()->GetLocalNodeInfo()->NodeID,false);
#ifdef HFS_KFS
	WriteItem((CFileDriverPtr)rec,"DATA",dob->GetFile(),false,true);
#endif
	rec->fclose();
	dob->fclose();

	return _ERR_SUCCESS;
}

hfsint CRecFile::WriteRef(hfsint64 id, hfsstring &sResult, hfsint iRecCount, hfsint iCiteCount, hfsint iCitedCount)
{
	rec = new CServerTempFile();
	if( !rec->fopen(GetRecName().c_str(),"w")){
		return _ERR_FILE_OPEN;
	}

	char cTaskID[64] = {0};

	SYSTEMTIME systime;
	GetLocalTime(&systime);
	char cTime[64] = {0};
	sprintf(cTime,"%04d-%02d-%02d:%02d:%02d:%02d",systime.wYear,systime.wMonth,systime.wDay,systime.wHour,systime.wMinute,systime.wSecond);

	WriteItem((CFileDriverPtr)rec,"RESULT",sResult,true);
	memset(cTaskID,0,sizeof(cTaskID));
	_itoa(iRecCount,cTaskID,10);
	WriteItem((CFileDriverPtr)rec,"REC_COUNT",cTaskID,false);
	memset(cTaskID,0,sizeof(cTaskID));
	_itoa(iCiteCount,cTaskID,10);
	WriteItem((CFileDriverPtr)rec,"CITE_COUNT",cTaskID,false);
	memset(cTaskID,0,sizeof(cTaskID));
	_itoa(iCitedCount,cTaskID,10);
	WriteItem((CFileDriverPtr)rec,"CITED_COUNT",cTaskID,false);
	WriteItem((CFileDriverPtr)rec,"COMPLETE_TIME",cTime,false);
	memset(cTaskID,0,sizeof(cTaskID));
	//_itoa(id,cTaskID,10);
	sprintf(cTaskID,"%I64d",id);
	WriteItem((CFileDriverPtr)rec,"TASKID",cTaskID,false);

	rec->fclose();

	return _ERR_SUCCESS;
}

hfststring	CRecFile::GetSQL(hfsstring table,hfsuint64 pos)
{
	if(! dob->fopen("rb")){
		return "";
	}

	hfsstring key = GetFileName();
	hfsstring ext = GetFileExt();
	hfsstring size = GetFileSize(dob);
	hfsstring date = GetCreateDate();
	hfsstring md5  = GetMD5();
	hfsstring dir  = GetDirName();

	hfschar sql[1024];
	sprintf(sql,"insert into %s (FILENAME,FILETYPE,FILESIZE,DIRNAME,CREATEDATE,MODIFYDATE,FLAG,MD5,NODEID,DATA) values('%s','%s','%s','%s','%s','%s','0','%s','%d','%I64d')",
		table.c_str(),
		key.c_str(),
		ext.c_str(),
		size.c_str(),
		dir.c_str(),
		date.c_str(),
		date.c_str(),
		md5.c_str(),
		CUtility::Config()->GetLocalNodeInfo()->NodeID,
		pos
	);

	dob->fclose();

	return sql;
}

hfsstring CRecFile::RecText(hfsuint64 pos)
{
	if(! dob->fopen("rb")){
		return "";
	}
	hfschar sdob[64];
	sprintf(sdob,"%I64d",pos);

	char rectext[2048];
	sprintf(rectext,"<REC>\n<FILENAME>=%s\n<FILETYPE>=%s\n<FILESIZE>=%s\n<DIRNAME>=%s\n<CREATEDATE>=%s\n<MODIFYDATE>=%s\n<MD5>=%s\n<FLAG>=%s\n<NODEID>=%d\n<DATA>=%s",
		GetFileName().c_str(),
		GetFileExt().c_str(),
		GetFileSize(dob).c_str(),
		GetDirName().c_str(),
		GetCreateDate().c_str(),
		GetModifyDate().c_str(),
		GetMD5().c_str(),
		"0",
		CUtility::Config()->GetLocalNodeInfo()->NodeID,
		sdob
		);

	dob->fclose();

	return rectext;
}

hfsint CRecFile::Write(hfsuint64 pos)
{
	rec = new CServerTempFile();
	if( !rec->fopen(GetRecName().c_str(),"w")){
		return _ERR_FILE_OPEN;
	}
	if(! dob->fopen("rb")){
		return _ERR_FILE_OPEN;
	}

	WriteItem((CFileDriverPtr)rec,"FILENAME",GetFileName(),true);
	WriteItem((CFileDriverPtr)rec,"FILETYPE",GetFileExt(),false);
	WriteItem((CFileDriverPtr)rec,"FILESIZE",GetFileSize(dob),false);
	WriteItem((CFileDriverPtr)rec,"DIRNAME",GetDirName(),false);
	WriteItem((CFileDriverPtr)rec,"CREATEDATE",GetCreateDate(),false);
	WriteItem((CFileDriverPtr)rec,"MODIFYDATE",GetModifyDate(),false);
	WriteItem((CFileDriverPtr)rec,"MD5",GetMD5(),false);
	WriteItem((CFileDriverPtr)rec,"FLAG","0",false);
	WriteItem((CFileDriverPtr)rec,"NODEID",CUtility::Config()->GetLocalNodeInfo()->NodeID,false);
#ifdef HFS_KDFS
	hfschar sdob[64];
	sprintf(sdob,"%I64d",pos);
	WriteItem((CFileDriverPtr)rec,"DATA",sdob,false);
#endif
	rec->fclose();
	dob->fclose();

	return _ERR_SUCCESS;
}

hfsint CRecFile::WriteItem(CFileDriverPtr fp, hfststring Name, hfststring Value, hfsbool bNewLine,hfsbool bDob)
{
	hfssize w=0;
	if(bNewLine){
		w+=fp->fwrite("<REC>",1,5);
		w+=fp->fwrite("\n",1,1);
	}
	w+=fp->fwrite("<",1,1);
	w+=fp->fwrite(Name.c_str(),1,(hfsint)Name.size());
	w+=fp->fwrite(">=",1,2);
	if(!bDob){
		w+=fp->fwrite(Value.c_str(),1,(hfsint)Value.size());
	}else{
		w+=fp->fwrite(Value.c_str(),1,(hfsint)Value.size());
		w+=fp->fwrite("#DOBFILE",1,8);
	}
	w+=fp->fwrite("\n",1,1);

	return (hfsint)w;
}

hfsint CRecFile::WriteItem(CFileDriverPtr fp, hfststring Name, int Value, hfsbool bNewLine,hfsbool bDob)
{
	hfssize w=0;
	if(bNewLine){
		w+=fp->fwrite("<REC>",1,5);
		w+=fp->fwrite("\n",1,1);
	}
	w+=fp->fwrite("<",1,1);
	w+=fp->fwrite(Name.c_str(),1,(hfsint)Name.size());
	w+=fp->fwrite(">=",1,2);
	char t[32];
	sprintf(t,"%d",Value);

	if(!bDob){	
		w+=fp->fwrite(t,1,(hfsint)strlen(t));
	}else{
		w+=fp->fwrite(t,1,(hfsint)strlen(t));
		w+=fp->fwrite("#DOBFILE",1,8);
	}
	w+=fp->fwrite("\n",1,1);

	return (hfsint)w;
}

hfststring CRecFile::GetTableName()
{
	vector<string> vc;
	CUtility::Str()->CStrToStrings(dob->GetFile().c_str(),vc,'\\');
	if((hfsint)vc.size() <= 1 ){
		return  CUtility::Config()->GetRootDir()+"_TEMP";
	}
	return vc[vc.size()-2];
}

hfststring CRecFile::GetFileName()
{
	vector<string> vc;
	CUtility::Str()->CStrToStrings(dob->GetFile().c_str(),vc,'\\');
	if((hfsint)vc.size() <= 1 ){
		return "";
	}
	hfschar cShorName[MAX_FILE_ID];
	strcpy(cShorName,vc[vc.size()-1].c_str());

	phfschar p = strrchr(cShorName,'.');
	if(p){
		*p ='\0';
	}
	return cShorName;
}

hfststring CRecFile::GetDirName()
{
	hfschar cFileName[MAX_FILE_ID];
	strcpy(cFileName,dob->GetFile().c_str());
	phfschar p = strrchr(cFileName,'\\');
	if(p){
		*p = '\0';
	}

	return CUtility::DiskName2SysName(cFileName);
}

hfststring CRecFile::GetFileExt()
{
	vector<string> vc;
	CUtility::Str()->CStrToStrings(dob->GetFile().c_str(),vc,'\\');
	if((hfsint)vc.size() <= 1 ){
		return "";
	}
	hfschar cShorName[MAX_FILE_ID];
	strcpy(cShorName,vc[vc.size()-1].c_str());

	phfschar p = strrchr(cShorName,'.');
	if(p){
		return ++p;
	}
	return "";
}

hfststring CRecFile::GetCreateDate()
{
	hfschar date[32];
	CUtility::GetLocalTime(date,"%04d-%02d-%02d %02d:%02d:%02d");
	return date;
}

hfststring CRecFile::GetModifyDate()
{
	hfschar date[32];
	CUtility::GetLocalTime(date,"%04d-%02d-%02d %02d:%02d:%02d");
	return date;
}

hfststring CRecFile::GetCRC32()
{
	hfsdword dw ;
	CCRC32::GetFileCrc32(dob->GetFile().c_str(),dw);
	hfschar crc[20];
	sprintf(crc,"%u",dw);
	return crc;
}

hfsstring CRecFile::GetMD5()
{
	CSmartMemoryPtr pSmart = new CSmartMemory();
	return CUtility::GetFileMD5(dob->GetFile().c_str(),pSmart->GetPtr(),pSmart->GetSize());
}

hfststring CRecFile::GetRecName()
{
	return dob->GetFile() + ".txt";
}

hfststring CRecFile::GetFileSize( CFileDriverPtr fp)
{
	hfsint64 pos = fp->ftell();
	fp->fseek64(0,2);
	hfsint64 len = fp->ftell();
	fp->fseek64(pos,0);
	
	hfschar size[20];
	sprintf(size,"%u",len);
	return size;
}