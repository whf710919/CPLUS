#include "StdAfx.h"
#include "Error.h"
#include "Utility.h"
#include "RecFile.h"
#include "HFSBase.h"
#include "SmartBuffer.h"
#include "rc4.h"

static CHFSBaseReaderPtr G_Reader = new CHFSBaseReader();
static CHFSBaseWriterPtr G_Writer = new CHFSBaseWriter();

CDOBFile::CDOBFile(hfsstring name)
{
	m_dob  = new CVirtualFile(name);
}

CDOBFile::~CDOBFile()
{
	if(m_dob){
		m_dob = (CVirtualFile*)0;
	}
}

void CDOBFile::InitHeader(HFS_DOB_HEADER* dob)
{
	HFS_DOB_HEADER header;
	memset(&header,0x00,sizeof(HFS_DOB_HEADER));
	strcpy(header.version,"1.0.1");
}

void CDOBFile::RC4(void*data, hfssize size,hfssize pos,hfssize ptr)
{
	static const char * m_key = "cnkiHFS123sqazwsx!@#@cnki.Com";
	static const int    key_size = strlen(m_key);
	if(!data || size <= 0 ){
		return;
	}

	if(pos >= 0 && pos <= 256){
		
		rc4_key rc4key;
		prepare_key((unsigned char*)m_key, key_size, &rc4key);
		size = size > 256?256:size;

		if(pos == 0 ){
			rc4((unsigned char*)data, size, &rc4key);
		}else{
			unsigned char encode[256];
			memset(encode,0x00,256);
			VPF()->Seek(0,ptr);
			hfsuint64 rbytes = VPF()->Read(encode,1,256);
			
			rc4(encode, (int)rbytes, &rc4key);
			VPF()->Seek(0,ptr+size);
			if(rbytes == 256){
				memcpy((char*)data,encode+pos,256-pos);
			}else{
				memcpy((char*)data,encode+pos,rbytes-pos);
			}
		}
	}
}

void CDOBFile::RC4(void* data, hfssize size ,hfssize pos)
{
	static const char * m_key = "cnkiHFS123sqazwsx!@#@cnki.Com";
	static const int    key_size = strlen(m_key);
	if(!data || size <= 0 ){
		return;
	}

	if(pos >= 0 && pos <= 256){
		
		rc4_key rc4key;
		prepare_key((unsigned char*)m_key, key_size, &rc4key);
		size = size > 256?256:size;

		if(pos == 0 ){
			rc4((unsigned char*)data, size, &rc4key);
		}else{
			unsigned char encode[256];
			memset(encode,0x00,256);
			VPF()->Seek(0,pos);
			hfsuint64 rbytes = VPF()->Read(encode,1,256);
			
			rc4(encode, (int)rbytes, &rc4key);
			VPF()->Seek(0,pos+size);
			if(rbytes == 256){
				memcpy((char*)data,encode+pos,256-pos);
			}else{
				memcpy((char*)data,encode+pos,rbytes-pos);
			}
		}
	}
}

hfsbool CDOBFile::Write(hfsstring local,hfsstring dob,hfsuint64& pos)
{
	G_Guard Lock(m_Lock);
	
	hfsint	e = _ERR_SUCCESS;
	pos = -1;
	hfsuint64 fsize = m_dob->FileSize();
	m_dob->Seek(fsize,0);

	CFileDriverPtr bin = new CFileDriver(local);
	if(!bin->fopen("rb")){
		//CUtility::Logger()->trace("CommitData can't open disk file(%s)",local.c_str());
		e = _ERR_FILE_OPEN;
	}

	CSmartMemoryPtr pSmart = new CSmartMemory();
	
	if(e >= _ERR_SUCCESS){	
		while( !bin->feof()){
			hfssize ptrpos = bin->ftell64();
			hfssize rsize = bin->fread(pSmart->GetPtr(),1,pSmart->GetSize());
			if(rsize <= 0 ){
				bin->fclose();
				//CUtility::Logger()->trace("CommitData read file error.");
				e = _ERR_FILE_READ;
				break;
			}

			RC4(pSmart->GetPtr(),rsize,ptrpos);

			hfssize wsize = m_dob->Write(pSmart->GetPtr(),1,rsize);

			if(wsize != rsize){
				bin->fclose();
				//CUtility::Logger()->trace("CommitData write file error.");
				e = _ERR_FILE_WRITE;
				break;
			}
		}
		bin->fclose();
	}

	if(!m_dob->Flush()){
		return false;
	}
	if( e >= _ERR_SUCCESS){
		pos = fsize;
	}
	return e >= _ERR_SUCCESS ? true:false;
}

CHFSBaseReader::CHFSBaseReader()
{

}

CHFSBaseReader::~CHFSBaseReader()
{
	G_Guard Lock(m_Lock);
	
	m_dobs.clear();
}

CDOBFilePtr CHFSBaseReader::GetFile(hfsstring dev)
{
	G_Guard Lock(m_Lock);
	map<hfsstring,CDOBFilePtr>::iterator it;

	hfschar name[MAX_FILE_ID];
	strcpy(name,dev.c_str());
	_strupr(name);

	it = m_dobs.find(name);
	if(it != m_dobs.end()){
		return it->second;
	}

	CDOBFilePtr dob = new CDOBFile(name);
	if(!dob->VPF()->Open(name,"rb+")){
		return 0;
	}

	m_dobs.insert(make_pair(name,dob));

	return dob;

}

hfsbool	CHFSBaseReader::ReleaseFile(hfsstring dev)
{
	G_Guard Lock(m_Lock);
	map<hfsstring,CDOBFilePtr>::iterator it;

	hfschar name[MAX_FILE_ID];
	strcpy(name,dev.c_str());
	_strupr(name);

	it = m_dobs.find(name);
	if(it == m_dobs.end()){
		return true;
	}

	CDOBFilePtr dob = it->second;
	dob->VPF()->Close();

	m_dobs.erase(it);

	return true;
}

CHFSBaseWriter::CHFSBaseWriter()
{

}

CHFSBaseWriter::~CHFSBaseWriter()
{
	G_Guard Lock(m_Lock);

	m_dobs.clear();
}

hfsbool	CHFSBaseWriter::ReleaseFile(hfsstring dev)
{
	G_Guard Lock(m_Lock);
	map<hfsstring,CDOBFilePtr>::iterator it;

	hfschar name[MAX_FILE_ID];
	strcpy(name,dev.c_str());
	_strupr(name);

	it = m_dobs.find(name);
	if(it == m_dobs.end()){
		return true;
	}

	CDOBFilePtr dob = it->second;
	dob->VPF()->Close();

	m_dobs.erase(it);

	return true;
}

CDOBFilePtr CHFSBaseWriter::GetFile(hfsstring dev)
{
	G_Guard Lock(m_Lock);
	map<hfsstring,CDOBFilePtr>::iterator it;

	hfschar name[MAX_FILE_ID];
	strcpy(name,dev.c_str());
	_strupr(name);

	it = m_dobs.find(name);
	if(it != m_dobs.end()){
		return it->second;
	}

	CDOBFilePtr dob = new CDOBFile(name);
	CUtility::DiskBase()->CreateDir(name);

	if(!dob->VPF()->Open(name,"rb+")){
		return 0;
	}

	m_dobs.insert(make_pair(name,dob));

	return dob;
}

CHFSBase::CHFSBase(void)
{

}


CHFSBase::~CHFSBase(void)
{
}

hfsint CHFSBase::CreateDir(pchfschar pDir)
{
	hfsint e = CKFSBase::CreateDir(pDir);
	if( e >= _ERR_SUCCESS) {
		
		G_Reader->GetFile(pDir);
		G_Writer->GetFile(pDir);
	}

	return e;
}

hfsint CHFSBase::DisableDir(pchfschar pDir)
{
	CKBConnPtr pCon = GetConn();
	if( !pCon ){
		return _ERR_KBASE_CONN;
	}
	hfschar cDirName[MAX_FILE_ID];
	strcpy(cDirName,pDir);
	if( cDirName[strlen(cDirName) -1] != '\\'){
		strcat(cDirName,"\\");
	}
	string dev = pCon->GetDevPath(cDirName);
	dev = GetDOBName(dev);
	G_Reader->ReleaseFile(dev);
	G_Writer->ReleaseFile(dev);
	ReleaseConn(pCon);

	hfsint e = CKFSBase::DisableDir(cDirName);
	
	return e;
}

hfsint CHFSBase::EnableDir(pchfschar pDir)
{
	hfsint e = CKFSBase::EnableDir(pDir);
	
	return e;
}

hfshandle CHFSBase::OpenFile(pchfschar pFullName, pchfschar mode)
{
	return CKFSBase::OpenFile(pFullName,mode);
}

hfsuint64 CHFSBase::SeekFile(hfshandle hHandle,hfsint64 iOffset,hfsuint uiOrigin)
{
	return CKFSBase::SeekFile(hHandle,iOffset,uiOrigin);
}

hfsuint CHFSBase::ReadFile(hfshandle hHandle,phfschar pBuffer,hfsuint uiSize)
{
	clock_t start,end;

	if(!hHandle){
		return 0;
	}

	start = clock();

	CKFSHanlePtr pHandle = GetHandle(hHandle);
	if(!pHandle){
		return _ERR_INVALID_HANDLE;
	}

	hfsstring sdev = GetDOBName(pHandle);
	if(sdev.empty()){
		return _ERR_INVALID_HANDLE;
	}
	
	//CUtility::Logger()->ltrace("DOBFILE(%s)",sdev.c_str());

	CDOBFilePtr dob = G_Reader->GetFile(sdev);
	if(!dob){
		return _ERR_FILE_READ;
	}
	HFS_FILE_INFO* hfs = &(pHandle->m_ifo);
	hfssize ptrpos = pHandle->GetPtr();

	if(ptrpos  >= (hfsuint64)hfs->uiFileSize || ptrpos < 0){
		return 0;
	}
	if( ptrpos + uiSize > (hfsuint64)hfs->uiFileSize){
		uiSize = (hfsuint)(hfs->uiFileSize - ptrpos);
	}
	if( uiSize <= 0 ){
		return 0;
	}
	G_Guard Lock(m_Lock);
	hfsint64 ptr = ptrpos + pHandle->m_ifo.pos.ulPos;
	CVirtualFilePtr fp = dob->VPF();
	if(!fp){
		return 0;
	}

	if(fp->Seek(ptr,0) < 0 ){
		//CUtility::Logger()->trace("Seek(%s,%I64d) error",sdev.c_str(),ptr);
		return 0;
	}

	hfsuint e = (hfsuint)fp->Read(pBuffer,1,uiSize);
	//CUtility::Logger()->trace("Read(%I64d,%d) %d",ptr,uiSize,e);
	if(e <= 0 ){
		return 0;
	}
	dob->RC4((void*)pBuffer,e,ptrpos,ptr);

	end = clock();

	//CUtility::Logger()->trace("ReadFile(%I64d,%I64d,%d):%d %d ms",hHandle,pHandle->GetPtr(),uiSize,e,end-start);

	if(e >= _ERR_SUCCESS){
		pHandle->SetPtr(pHandle->GetPtr()+e);
		return (hfsuint)e;
	}
	return 0;
}

hfsuint CHFSBase::ReadFile(hfshandle hHandle,hfsint64 pos,phfschar pBuffer,hfsuint uiSize)
{
	clock_t start,end;
	if(!hHandle){
		return 0;
	}
	start = clock();
	
	CKFSHanlePtr pHandle = GetHandle(hHandle);
	if(!pHandle){
		return _ERR_INVALID_HANDLE;
	}

	//CUtility::Logger()->trace("ReadFile(%s,%I64d) start",pHandle->m_ifo.tsFileName,pos);

	hfsstring sdev = GetDOBName(pHandle);
	if(sdev.empty()){
		return _ERR_INVALID_HANDLE;
	}

	CDOBFilePtr dob = G_Reader->GetFile(sdev);
	if(!dob){
		return _ERR_FILE_READ;
	}

	if( pos < 0 ){
		pos = pHandle->GetPtr();
	}

	HFS_FILE_INFO* hfs = &(pHandle->m_ifo);
	if( pos >= hfs->uiFileSize || pos < 0){
		return 0;
	}

	if( pos + uiSize > (hfsuint64)hfs->uiFileSize){
		uiSize = (hfsuint)(hfs->uiFileSize - pos);
	}
	if( uiSize <= 0 ){
		return 0;
	}

	G_Guard Lock(m_Lock);
	CVirtualFilePtr fp = dob->VPF();
	hfssize ptr = pHandle->m_ifo.pos.ulPos+pos;


	hfsuint e = fp->Seek(ptr,0);
	if( e < 0 ){
		//CUtility::Logger()->trace("Seek(%s,%I64d) error",sdev.c_str(),ptr);
		return 0;
	}
	e = (hfsuint)fp->Read(pBuffer,1,uiSize);
	//CUtility::Logger()->trace("Read(%I64d,%d) %d",ptr,uiSize,e);

	if(e <= 0 ){
		return 0;
	}

	dob->RC4((void*)pBuffer,e,pos,ptr);

	end = clock();

	//CUtility::Logger()->trace("ReadFile(%s,%I64d,%d):%d %d ms",pHandle->m_ifo.tsFileName,pos,uiSize,e,end-start);

	if(e >= _ERR_SUCCESS){
		pHandle->SetPtr(pos+e);
		return e;
	}
	return 0;
}

hfsstring CHFSBase::GetDOBName(string TableName)
{
	if(TableName.empty()){
		return "";
	}

	TableName = CUtility::DiskName2SysName(TableName);
	TableName = CUtility::SystemName2DiskName(TableName,!CUtility::IsSyncDir((phfschar)TableName.c_str()));
	
	hfschar sdev[MAX_FILE_ID];
	strcpy(sdev,TableName.c_str());
	strcat(sdev,".dob");

	return sdev;
}

hfsstring CHFSBase::GetDOBName(CKFSHanlePtr pHandle)
{
	if(!pHandle || !pHandle->GetConn()){
		return "";
	}
	hfsstring dev;
	if(pHandle->IsWrite()){
		string sFileName = pHandle->fp()->GetFile();
		dev = pHandle->GetConn()->GetDevPath(sFileName.c_str());
	}else{
		dev = pHandle->GetConn()->GetDevPath();
	}

	return GetDOBName(dev);
}

hfsbool CHFSBase::Write2DOB(CKFSHanlePtr pHandle,hfsuint64& pos )
{
	hfsint e = _ERR_SUCCESS;
	pos = -1;
	clock_t start,end;
	hfsbool bSuccess = true;
	start = clock();
	string sFileName;

	if(pHandle->fp()){
		sFileName = pHandle->fp()->GetFile();
		pHandle->fp()->fclose();

		hfsstring sdev = GetDOBName(pHandle);

		if(pHandle->IsWrite()){

			CDOBFilePtr dob = G_Writer->GetFile(sdev);
			if(!dob){
				//CUtility::Logger()->trace("CommitData can't open disk file(%s)",sdev.c_str());
				bSuccess = false;
			}

			if( !dob->Write(sFileName,sdev,pos)){
				pos = -1;
				bSuccess = false;
			}
		}
	}

	if(pos >= 0 ){
		bSuccess = true;
	}else{
		bSuccess = false;
	}

	end = clock();

	//CUtility::Logger()->trace("Write2DOB(%s,%I64d):%d ms",sFileName.c_str(),pos,end-start);

	return bSuccess;
}

hfsint CHFSBase::CommitData(CKFSHanlePtr pHandle)
{
	hfsint e = _ERR_SUCCESS;
	clock_t start,end;
	start = clock();
	string sFileName;
	if(!pHandle || !pHandle->GetConn()){
		return _ERR_INVALID_HANDLE;
	}
	if(pHandle->fp()){
		sFileName = pHandle->fp()->GetFile();
		pHandle->fp()->fclose();

		if(pHandle->IsWrite()){
			CKBConnPtr pCon = pHandle->GetConn();
			
			string sDevName = GetDOBName(pHandle);

			e = CreateDir(sDevName.c_str());
			if( e < _ERR_SUCCESS){
				return e;
			}
			
			hfsuint64 pos = 0;
			if(!Write2DOB(pHandle,pos)){
				return _ERR_FILE_WRITE;
			}

			e = pCon->DeleteFile(sFileName.c_str());

			if( e >= _ERR_SUCCESS){
				e = pCon->WriteFile(sFileName,pos);
			}

			if(e >= _ERR_SUCCESS){
				string sDirName = pCon->GetFileTable(sFileName.c_str());
				AddNotify( sDirName.c_str());
			}
		}
	}

	end = clock();

	//CUtility::Logger()->trace("CommitData(%s):%d ms",sFileName.c_str(),end -start);

	return e;
}

int	CHFSBase::DeleteDir(pchfschar pDir)
{
	CKBConnPtr pCon = GetConn();
	if( !pCon ){
		return _ERR_KBASE_CONN;
	}
	string sTableName = pCon->GetDevPath(pDir);
	ReleaseConn(pCon);

	string sDobFile = GetDOBName(sTableName);
	if(sDobFile.empty()){
		return _ERR_SUCCESS;
	}

	G_Reader->ReleaseFile(sDobFile);
	G_Writer->ReleaseFile(sDobFile);

	return CKFSBase::DeleteDir(pDir);
}

hfsint CHFSBase::WriteFile(hfshandle hHandle,pchfschar pBuffer,hfsuint uiSize,hfsint64 pos)
{
	return CKFSBase::WriteFile(hHandle,pBuffer,uiSize,pos);
}