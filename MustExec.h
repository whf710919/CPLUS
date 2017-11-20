#ifndef HFS_MUSTEXEC_H_
#define HFS_MUSTEXEC_H_

#ifdef _WINDOWS_
#include <windows.h>
#else
#include <windows.h>
#endif
typedef hfsint (* MyFun)();
class MustExec
{
public:
	MustExec(MyFun myconfun,MyFun mydefun):m_myConFun(myconfun),m_myDeFun(mydefun){ m_myConFun(); };
	virtual ~MustExec(){ m_myDeFun(); };
private:
	MyFun m_myConFun;
	MyFun m_myDeFun;
};
#endif