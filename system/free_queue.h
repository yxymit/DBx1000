#pragma once

/*
#include "helper.h"
#include "global.h"
//#include <boost/lockfree/queue.hpp>
#include <boost/lockfree/spsc_queue.hpp>
//#include <queue>

class FreeQueue  
{
public:
	void * get_element() {
		void * t = NULL;
		_free_elements.pop(t);
		return t;
	};
	void return_element(void * t) {
		_free_elements.push(t);
	}
private:
	//std::queue<void *> _free_elements;
	//boost::lockfree::queue<void *> _free_elements{32};
	boost::lockfree::spsc_queue<void *> _free_elements{32};
};

*/
