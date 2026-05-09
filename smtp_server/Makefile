CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc
TARGET   := smtp_server
SRC      := src/smtp_server.cc
LDLIBS   := -lresolv

# Link libcurl only if available; compiles without it (relay mode disabled).
CURL_OK := $(shell pkg-config --exists libcurl 2>/dev/null && echo yes)
ifeq ($(CURL_OK),yes)
    CXXFLAGS += -DUSE_CURL_RELAY $(shell pkg-config --cflags libcurl)
    LDLIBS   += $(shell pkg-config --libs libcurl)
else ifneq ($(wildcard /usr/include/curl/curl.h),)
    CXXFLAGS += -DUSE_CURL_RELAY
    LDLIBS   += -lcurl
endif

all: $(TARGET)

$(TARGET): $(SRC) src/smtp_client.h
	$(CXX) $(CXXFLAGS) -o $@ $(SRC) $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
