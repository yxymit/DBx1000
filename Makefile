CC=g++
CFLAGS=-Wall -g -std=c++0x

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./include

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -O3 -D_GNU_SOURCE
LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread  -lrt -ljemalloc 
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS)
	$(CC) $(ARCH) -o $@ $^ $(LDFLAGS)

#We don't need to clean up when we're making these targets
NODEPS:=clean
ifeq (0, $(words $(findstring $(MAKECMDGOALS), $(NODEPS))))
    -include $(OBJS:%.o=%.d)
endif



%.d: %.cpp
	$(CC) $(ARCH) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp %.d
	$(CC) $(ARCH) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)


