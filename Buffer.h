#ifndef _HFS_BUFFER_H_
#define _HFS_BUFFER_H_
#include "DataDef.h"
enum
{
	RECV_BUFFER,
	SEND_BUFFER,
};

class CBuffer
{
public:
	CBuffer(int nType=RECV_BUFFER);
	virtual ~CBuffer(void);

	inline int GetBufferLen()
	{
		return (m_pBase!=NULL)?(int)(m_pPtr-m_pBase):0;
	}

	inline  int	GetMemSize()
	{
		return (m_pBase!=NULL)?m_nSize:0;
	}

	inline void	ClearBuffer()
	{
		if(m_pBase != NULL)
		{
			*m_pBase='\0';
		}
		m_pPtr=m_pBase;
	}

	inline  char	*GetBuff()
	{
		return m_pBase;
	}

	bool	Write(char *pData, int nSize);
	
	char	*ReAllocateBuffer(int nRequestedSize);
	char	*DeAllocateBuffer(int nRequestedSize);

	inline  int		GetBufferType()
	{
		return m_nBufferType;
	}

// Attributes
protected:
	char 	*m_pBase;
	char	*m_pPtr;
	int		m_nSize;
	int		m_nBufferType;
};

#endif