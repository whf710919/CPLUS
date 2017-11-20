#ifndef HFS_TEXTGET_H_
#define HFS_TEXTGET_H_

#include "DataDef.h"
#include "NTService.h"

class CServer;
class CRefCount:public CNTService
{
public:
	CRefCount(pchfschar pServiceName, pchfschar pDisplayName);

	~CRefCount(void);
	
	virtual void	Run(DWORD, LPTSTR *);
	virtual void	Pause();
	virtual void	Continue();
	virtual void	Shutdown();
	virtual void	Stop();
	CServer			*m_pServer;
private:
	HANDLE			m_hStop;
};

extern CRefCount *G_Service;

#endif