#include "stdafx.h"
#include "RFD.h"
#include "KBase.h")

CKBConn *pKBase;
CRFD::CRFD(void)
{
	pKBase = new CKBConn();
	pKBase->OpenConn();
}


CRFD::~CRFD(void)
{
	pKBase->CloseConn();

	delete pKBase;
}

int Int(char *c)
{
	char szSql[1024];
	sprintf( szSql, "select SYS_FLD_CLASS_CODE from zjcls where SYS_FLD_CLASS_GRADE=2 and SYS_FLD_CLASS_CODE=%s?", c );
	TPI_HRECORDSET hset = pKBase->OPe
}

