CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -MMD -MP

UNAME_S := $(shell uname -s)
LDFLAGS := -pthread
LDLIBS :=

# Optional libcurl-backed SMTP relay path. Off by default so the build does
# not require libcurl. Enable with: make USE_CURL=1
USE_CURL ?= 0

ifeq ($(UNAME_S),Linux)
    LDLIBS += -lresolv
endif

ifeq ($(UNAME_S),Darwin)
    LDLIBS += -lresolv
endif

ifeq ($(USE_CURL),1)
    CXXFLAGS += -DPENN_USE_CURL
    LDLIBS += -lcurl
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
