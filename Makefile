CC=g++
CFLAGS=-Wall -g -std=c++0x

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
DEPS = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system

CFLAGS += $(DEPS) -D NOGRAPHITE=1 -Werror
LDFLAGS = -Wall -L. -pthread -g -lrt -std=c++0x 
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir,$(SRC_DIRS),$(wildcard $(dir)*.cpp)) 

#CPPS = $(wildcard *.cpp)
OBJS = $(addprefix obj/, $(notdir $(CPPS:.cpp=.o)))

#NOGRAPHITE=1

all:rundb

.PHONY: deps
deps:$(CPPS)
	$(CC) $(CFLAGS) -MM $^ > obj/deps
	sed '/^[^ ]/s/^/obj\//g' obj/deps > obj/deps.tmp
	mv obj/deps.tmp obj/deps
-include obj/deps

rundb : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)
#./deps/%.d: %.cpp
#	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<
./obj/%.o: benchmarks/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: storage/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: system/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: concurrency_control/%.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<
./obj/%.o: %.cpp
	$(CC) -c $(CFLAGS) $(INCLUDE) -o $@ $<

.PHONY: clean
clean:
	rm -f obj/*.o obj/.depend rundb
