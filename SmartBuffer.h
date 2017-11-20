#include "DataDef.h"
#include "smartptr.h"
#include "BuffMgr.h"

class CSmartMemory:public CRefCounter
{
public:
	CSmartMemory(void);
	~CSmartMemory(void);
	void*	GetPtr();
	hfsint	GetSize();
private:
	CBuff	*m_Data;
};

typedef Ptr<CSmartMemory> CSmartMemoryPtr;

