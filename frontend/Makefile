CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -MMD -MP

UNAME_S := $(shell uname -s)
LDFLAGS := -pthread
LDLIBS :=

ifeq ($(UNAME_S),Linux)
    LDLIBS += -lresolv
endif

ifeq ($(UNAME_S),Darwin)
    LDLIBS += -lresolv
endif

SRCS := src/main.cc src/fe_server.cc src/handlers_mail.cc
OBJS := $(SRCS:src/%.cc=build/%.o)
DEPS := $(OBJS:.o=.d)

TARGET := feserver

all: $(TARGET)

build/%.o: src/%.cc
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c -o $@ $<

$(TARGET): $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $(OBJS) $(LDLIBS)

clean:
	rm -rf build $(TARGET)

-include $(DEPS)
