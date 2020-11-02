HOSTNAME = $(shell hostname)

CC=g++ # -4.8

ifeq ($(HOSTNAME), home-desktop)
CC=g++
endif


ifeq ($(shell echo $(HOSTNAME) | head -c 2), ip)
CC=g++ # EC2
endif

CFLAGS=-Wall -g -std=c++0x # -rdynami

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./include

CFLAGS += $(INCLUDE) -lrt -lpthread -msse4.2 -march=native -ffast-math -Werror -O3 -D_GNU_SOURCE -fopenmp # -fsanitize=address # -D_FORTIFY_SOURCE=1 -fsanitize=address # -std=c++11 -fsanitize=address -fno-omit-frame-pointer

ifneq ($(JE_MALLOC), NO) # by default we use jemalloc
UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
RELEASE_NAME := $(shell uname -r | cut -d '-' -f 3)
	ifeq ($(RELEASE_NAME),generic)
        LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -ljemalloc -lnuma
	endif
	ifeq ($(RELEASE_NAME),aws)
	# changing to aws will hurt TC ycsb performance (9.xe6 to 6.7e6)???
        LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -ljemalloc -lnuma # -lasan
	endif
	ifeq ($(RELEASE_NAME),Microsoft) # if Windows Subsystem
		LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt # -ljemalloc
	endif
endif
ifeq ($(UNAME_S),Darwin)
        LDFLAGS = -Wall -L. -g -ggdb -std=c++0x -O3 -pthread  -lSystem.B # -ljemalloc 
endif
else
	 LDFLAGS = -Wall -L. -L./libs -g -ggdb -std=c++0x -O3 -pthread -lrt -lnuma # -lasan
endif

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


