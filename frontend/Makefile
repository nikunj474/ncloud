CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -MMD -MP

UNAME_S := $(shell uname -s)
LDFLAGS := -pthread
LDLIBS  := -lresolv

# Link libcurl only if available; compiles without it (relay mode disabled).
CURL_OK := $(shell pkg-config --exists libcurl 2>/dev/null && echo yes)
ifeq ($(CURL_OK),yes)
    CXXFLAGS += -DUSE_CURL_RELAY $(shell pkg-config --cflags libcurl)
    LDLIBS   += $(shell pkg-config --libs libcurl)
else ifneq ($(wildcard /usr/include/curl/curl.h),)
    CXXFLAGS += -DUSE_CURL_RELAY
    LDLIBS   += -lcurl
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
