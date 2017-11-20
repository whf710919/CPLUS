#include "StdAfx.h"
#include "ServerTempFile.h"


CServerTempFile::CServerTempFile(void):CFileDriver()
{

}


CServerTempFile::~CServerTempFile(void)
{
	remove();
}
