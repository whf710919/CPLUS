#include "StdAfx.h"
#include "CTaskFactory.h"
#include "Error.h"
#include "Task.h"

CTaskFactory::CTaskFactory()
{

}

CTaskFactory::~CTaskFactory()
{

}

hfsbool CTaskFactory::CreateInstance(hfsint iid,CTaskPtr & p)
{
	CTaskPtr task(0);

	task = new CTask();
	if( task){
		p = task;
		return true;
	}

	return false;
}
	
CTaskList::CTaskList()
{
	m_Tasks.clear();
}

CTaskList::~CTaskList()
{
	m_Tasks.clear();
}

CTaskPtr CTaskList::Get()
{
	G_Guard Lock(m_Lock);
	CTaskPtr t(0);
	map<hfsint64,CTaskPtr>::iterator it;
	for( it = m_Tasks.begin(); it != m_Tasks.end(); ++it){
		if( it->second->T()->nState == CTask::T_WAIT){
			t = it->second;
			t->T()->nState = CTask::T_RUN;
			break;
		}
	}
	return t;
}

hfsint64 CTaskList::Add(CTaskPtr task,hfsbool bOnlyChache)
{
	G_Guard Lock(m_Lock);
	vector<hfsint64>::iterator it;
	it = find( m_TaskIndex.begin(), m_TaskIndex.end(), task->T()->nStatType );
	if( it == m_TaskIndex.end() )
	{
		m_Tasks.insert(make_pair(task->T()->nStatType,task));
		m_TaskIndex.push_back(task->T()->nStatType);

		return task->T()->nStatType;
	}
	else
	{
		return 0;
	}

}

void CTaskList::Remove(hfsint64 taskid)
{
	G_Guard Lock(m_Lock);
	map<hfsint64,CTaskPtr>::iterator it;
	vector<hfsint64>::iterator itt;

	if( taskid > 0 ){

		it = m_Tasks.find(taskid);
		if( it != m_Tasks.end()){
			m_Tasks.erase(it);

			itt = find(m_TaskIndex.begin(),m_TaskIndex.end(),taskid);
			if( itt != m_TaskIndex.end()){
				m_TaskIndex.erase(itt);
			}
		}
	}
}

void CTaskList::Remove(CTaskPtr task)
{
	G_Guard Lock(m_Lock);
	Remove(task->T()->nStatType);
}


CTaskPtr CTaskList::GetTask(hfsint64 taskid)
{
	G_Guard Lock(m_Lock);
	map<hfsint64,CTaskPtr>::iterator it;

	it = m_Tasks.find(taskid);
	if(it != m_Tasks.end()){
		return it->second;
	}
	
	return 0;
}

DOWN_STATE* CTaskList::Get(hfsint64 taskid)
{
	G_Guard Lock(m_Lock);
	CTaskPtr t = GetTask(taskid);
	if(t){
		return t->T();
	}
	return 0;
}
