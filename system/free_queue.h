#pragma once

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>

//template <class T>
class FreeQueue  
{
public:
	void * get_element();
	void return_element(void * t);
private:
	boost::lockfree::queue<void *> _free_elements{8};
};
