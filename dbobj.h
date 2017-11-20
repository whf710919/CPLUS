
#if !defined(__CDbObj_h_)
#define __CDbObj_h_

#include "smartptr.h"


#ifndef byte
typedef unsigned char byte;
#endif

class  CDbObj : public CRefCounter
{
public:

	CDbObj(void);
	CDbObj(void* pd, size_t sz);
	CDbObj(const std::string& s);
	CDbObj(const char* ps, size_t sz = 0);
	CDbObj(unsigned long ul);
	CDbObj(long l);
	CDbObj(unsigned short us);
	CDbObj(short s);
	CDbObj(size_t ll);
	CDbObj(CDbObj& obj);
	~CDbObj();
	CDbObj& operator=(CDbObj& obj);
public:
	const void* GetData() const;
	size_t GetSize() const;
	void SetData(const void* pd, size_t sz);
private:
	byte* m_data;
	size_t m_size;
};

typedef Ptr<CDbObj> CDbObjPtr;
typedef vector<CDbObjPtr> DBOBJVECTOR;
typedef list<CDbObjPtr> DBOBJLIST;


#endif
