#include "log.h"
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>


const int buff_size = 10;

uint64_t global_lsn = 0;

struct log_record{
  uint64_t lsn;
  uint64_t txn_id;
  uint64_t key;
  uint64_t length;
  char * after_image;
};

struct log_record buffer[buff_size];
int buff_index = 0;

timespec commit_time;

timespec getTime()
{
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts); // Works on Linux
  return ts;  
}

void
LogManager::logTxn( uint64_t txn_id, uint64_t key, uint32_t length, char * after_image )
{
  // cout << "Log txn";
  // get a global LSN
  uint64_t lsn = global_lsn;
  global_lsn ++;
  // generate a log record.
  // put the log record into a buffer.
  buffer[buff_index].lsn = lsn;
  buffer[buff_index].txn_id = txn_id;
  buffer[buff_index].key = key;
  buffer[buff_index].length = length;
  buffer[buff_index].after_image = after_image;
  if (buff_index >= buff_size)
    {
      flushLogBuffer();
    }
  // if the buffer is full or times out, 
  //    flush the buffer to disk
  //    update the commit time for flushed transactions
  // else
  //    return
  return;
}

void LogManager::flushLogBuffer()
{
  ofstream log;
  log.open("Log.data", ios::binary);
  for (int i=0; i<buff_size; i++)
    {
      log.write((char*)&buffer[i].lsn, sizeof(buffer[i].lsn));
      log.write((char*)&buffer[i].txn_id, sizeof(buffer[i].txn_id));
      log.write((char*)&buffer[i].key, sizeof(buffer[i].key));
      log.write((char*)&buffer[i].length, sizeof(buffer[i].length));
      log.write(buffer[i].after_image, buffer[i].length);
    }
  log.close();
  commit_time = getTime();
  buff_index = 0;
}

void LogManager::readFromLog()
{
  streampos size;
  char * memblock;
  ifstream file ("example.bin", ios::in|ios::binary|ios::ate);
  if (file.is_open())
    {
      size = file.tellg();
      memblock = new char [size];
      file.seekg (0, ios::beg);
      file.read (memblock, size);
      file.close();
      cout << "MEMBLOCK " << memblock;
    }
  else cout << "Unable to open file";
}


