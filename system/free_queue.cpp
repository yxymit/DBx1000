#include "free_queue.h"

//template <class T>
void * 
FreeQueue::get_element()
{
	void * t = NULL;
	_free_elements.pop(t);
	return t;
}

//template <class T>
void 
FreeQueue::return_element(void * t)
{
	_free_elements.push(t);
}
