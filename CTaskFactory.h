#ifndef HFS_TASKFACTORY_H_
#define HFS_TASKFACTORY_H_

#include "DataDef.h"
#include "smartptr.h"
#include "Task.h"

class CTaskFactory:public CRefCounter
{
public:
	CTaskFactory();
	~CTaskFactory();
	hfsbool	CreateInstance(hfsint iid,CTaskPtr & p);
};

typedef Ptr<CTaskFactory> CTaskFactoryPtr;

class CTaskList:public CRefCounter
{
public: 
	CTaskList();
	~CTaskList();
	CTaskPtr			Get();
	hfsint64			Add(CTaskPtr task,hfsbool bOnlyChache = false);
	void				Remove(CTaskPtr task);
	void				Remove(hfsint64 taskid);
	DOWN_STATE*			Get(hfsint64 taskid);
	CTaskPtr			GetTask(hfsint64 taskid);
private:
	hfsint64			m_TaskCounter;
	map<hfsint64,CTaskPtr>m_Tasks;

	vector<hfsint64>	  m_TaskIndex;

	G_Lock				m_Lock;
	CTaskFactoryPtr     m_TaskFactory;
};

typedef Ptr<CTaskList> CTaskListPtr;

#endif