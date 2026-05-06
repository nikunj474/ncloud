# Build the standalone KV storage server binary.
CXX := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc -MMD -MP

UNAME_S := $(shell uname -s)

# std::filesystem link handling:
# - macOS: do NOT use -lstdc++fs
# - Linux: usually safe without it on modern compilers, but keep an easy switch
LDFLAGS := -pthread

ifeq ($(UNAME_S),Linux)
    # Uncomment this only if your Linux toolchain complains about filesystem symbols
    # LDFLAGS += -lstdc++fs
endif

SRCS := src/tablet.cc src/server.cc src/main.cc
OBJS := $(SRCS:src/%.cc=build/%.o)
DEPS := $(OBJS:.o=.d)

TARGET := kvserver

all: $(TARGET)

# Compile each C++ source file into a dependency-tracked object file.
build/%.o: src/%.cc
	mkdir -p build
	$(CXX) $(CXXFLAGS) -c -o $@ $<

# Link the tablet, TCP server, and CLI entry point into kvserver.
$(TARGET): $(OBJS)
	$(CXX) $(LDFLAGS) -o $@ $(OBJS)

# Remove generated objects, dependency files, and the kvserver binary.
clean:
	rm -rf build $(TARGET)

-include $(DEPS)
