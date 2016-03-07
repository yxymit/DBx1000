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

LogManager::LogManager()
{
    // TODO [YXY] Open the log file here.
}

void 
LogManager::logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
  // TODO [YXY] Should lock the log manager
  uint64_t lsn = global_lsn;
  global_lsn ++;
  // generate a log record.
  // put the log record into a buffer.
  buffer[buff_index].lsn = lsn;
  buffer[buff_index].txn_id = txn_id;
  // added [0] just to make it compile
  buffer[buff_index].key = keys[0]; 	
  buffer[buff_index].length = lengths[0];
  // TODO [YXY] copy data, not ptr 
  buffer[buff_index].after_image = after_images[0];
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
      // TODO [YXY] Flush the whole buffer at the same time.
      log.write((char*)&buffer[i].lsn, sizeof(buffer[i].lsn));
      log.write((char*)&buffer[i].txn_id, sizeof(buffer[i].txn_id));
      log.write((char*)&buffer[i].key, sizeof(buffer[i].key));
      log.write((char*)&buffer[i].length, sizeof(buffer[i].length));
      log.write(buffer[i].after_image, buffer[i].length);
    }
  log.close();
  buff_index = 0;
}

bool
LogManager::readFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, char ** &after_image)
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
	  return true;
  }
  else
	  return false;
}


