// stdafx.h : include file for standard system include files,
// or project specific include files that are used frequently, but
// are changed infrequently
//

#pragma once

#include "targetver.h"

#include <winsock2.h>
#include <mswsock.h>

#pragma comment(lib, "ws2_32.lib")  

#include <stdio.h>
#include <tchar.h>
#include <vector>
#include <list>
#include <string>
#include <map>
using namespace std;

#include <time.h>
#include "smartptr.h"

//#define HFS_KFS		0 //KBASE �洢�ļ�ϵͳ
#define HFS_KDFS	1 //KBASE�ʹ��̷ּ��洢ϵͳ
//#define HFS_HDFS	2 //���̴洢ϵͳ

#pragma warning(disable:4996)

// TODO: reference additional headers your program requires here
