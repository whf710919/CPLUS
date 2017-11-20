
#include "stdafx.h"

#include "dbobj.h"


CDbObj::CDbObj(void)
{
	m_data = 0;
	m_size = 0;
}

CDbObj::CDbObj(void* pd, size_t sz)
{
	m_size = sz;
	if (m_size != 0){
		m_data = new byte[m_size];
		memcpy(m_data, pd, m_size);
	}
}

CDbObj::CDbObj(const string& s)
{
	m_size = s.length();
	m_data = new byte[m_size];
	memcpy(m_data, s.c_str(), m_size);
}

CDbObj::CDbObj(const char* ps, size_t sz)
{
	m_size = (0 == sz) ? strlen(ps) : sz;
	m_data = new byte[m_size];
	memcpy(m_data, ps, m_size);
}

CDbObj::CDbObj(unsigned long ul)
{
	m_size = sizeof(unsigned long);
	m_data = new byte[m_size];
	*((unsigned long*)m_data) = ul;
}

CDbObj::CDbObj(size_t ll)
{
	m_size = sizeof(size_t);
	m_data = new byte[m_size];
	*((size_t*)m_data) = ll;
}

CDbObj::CDbObj(long l)
{
	m_size = sizeof(long);
	m_data = new byte[m_size];
	*((long*)m_data) = l;
}

CDbObj::CDbObj(unsigned short us)
{
	m_size = sizeof(unsigned short);
	m_data = new byte[m_size];
	*((unsigned short*)m_data) = us;
}

CDbObj::CDbObj(short s)
{
	m_size = sizeof(short);
	m_data = new byte[m_size];
	*((short*)m_data) = s;
}

CDbObj::CDbObj(CDbObj& obj)
{
	m_data = 0;
	m_size = 0;
	operator=(obj);
}

CDbObj::~CDbObj()
{
	if (m_size != 0){
		delete[] m_data;
	}
	m_data = 0;
	m_size = 0;
}

CDbObj& CDbObj::operator=(CDbObj& obj)
{
	if (m_size != 0){
		delete[] m_data;
	}
	if (obj.m_size > 0){
		m_size = obj.m_size;
		m_data = new byte[m_size];
		memcpy(m_data, obj.m_data, m_size);
	}
	return (*this);
}

const void* CDbObj::GetData() const
{ 
	return m_data; 
}

size_t CDbObj::GetSize() const
{ 
	return m_size; 
}

void CDbObj::SetData(const void* pd, size_t sz)
{
	if (m_size){
		delete[] m_data;
	}
	m_size = sz;
	m_data = new byte[m_size];
	memcpy(m_data, pd, m_size);
}