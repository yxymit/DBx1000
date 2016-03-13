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
#include<pthread.h>

pthread_mutex_t lock;

const int buff_size = 10;

uint64_t global_lsn = 0;

struct log_record{
  uint64_t lsn;
  uint64_t txn_id;
  uint32_t num_keys;
  string * table_names;
  uint64_t * keys;
  uint32_t * lengths;
  char ** after_images;
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
  //  cout << "Entered logTxn";
  // Lock log manager
  pthread_mutex_lock(&lock);
  
  uint64_t lsn = global_lsn;
  global_lsn ++;
  buff_index ++;
 
 
  buffer[buff_index].lsn = lsn;
  buffer[buff_index].txn_id = txn_id;
  
  buffer[buff_index].table_names = table_names;
  buffer[buff_index].num_keys = num_keys;
  buffer[buff_index].keys = keys;
  // Should all lengths be copied?
  buffer[buff_index].lengths = lengths;
  buffer[buff_index].after_images = (char**) malloc(num_keys*sizeof(char *));
  for (uint32_t a=0; a<num_keys; a++)
    {
      buffer[buff_index].after_images[a] = (char *) malloc(lengths[a]);
      for (uint32_t b=0; b<lengths[a]; b++)
	buffer[buff_index].after_images[a][b] = after_images[a][b];
    }
      
  if (buff_index > buff_size)
    {
      flushLogBuffer();
    }
  pthread_mutex_unlock(&lock);
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
  log.open("Log.data", ios::binary|ios::app);
  for (int i=0; i<buff_size; i++)
    {
      log.write((char *) &(buffer[i].lsn), sizeof(buffer[i].lsn));
      log.write((char *) &(buffer[i].txn_id), sizeof(buffer[i].txn_id));
      log.write((char *) &(buffer[i].num_keys), sizeof(buffer[i].num_keys));
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  int n = buffer[i].table_names[j].length();
	  log.write((char *) &n, sizeof(n));
	  log.write((char *) &(buffer[i].table_names[j]), n); 
	}
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	    log.write((char *) &(buffer[i].keys[j]), sizeof(buffer[i].keys[j]));
	}
    
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  log.write((char *) &(buffer[i].lengths[j]), sizeof(buffer[i].lengths[j]));
	}
      
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  log.write(buffer[i].after_images[j], buffer[i].lengths[j]);
	}
    }
  log.flush();
  log.close();
  buff_index = 0;
}

bool
LogManager::readFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, char ** &after_image)
{ 
  // return true if the end of the file has not been reached. 
  // return false if the end has been reached.

  // You should have noticed that the input parameters in this function are
  // almost identical to that of function logTxn(). This function (readFromLog) 
  // is the reverse of logTxn(). logTxn() writes the information to a file, and readFromLog()
  // recovers the same information back.  
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


