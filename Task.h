#ifndef REF_TASK_H_
#define REF_TASK_H_

#include "DataDef.h"
#include "smartptr.h"
#include "GLock.h"
#include "CvirtualFile.h"
#include "Error.h"

class CTask;
typedef Ptr<CTask> CTaskPtr;

class CTask : public CRefCounter
{
public:
	enum TaskType
	{
		PURE_TEXT	= 0x1,
		E_LEARNING	= 0x2,
		REFWORKS	= 0x4,
		ENDNOTE		= 0x8,
		REFERENCE	= 0x10,
		BIB			= 0x20
	}TaskType;

	enum TaskState
	{
		T_WAIT		= 0x1,
		T_RUN		= 0x2,
		T_FINISH	= 0x4,
		T_CANCEL	= 0x8,
		T_ERROR		= 0x10
	}TaskState;
public:
	CTask(void);
	~CTask(void);
	DOWN_STATE*		T(){return &m_Task;}
private:
	DOWN_STATE		m_Task;
};

#endif


