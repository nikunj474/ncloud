CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
LDFLAGS  := -pthread 
TARGET   := coordinator

all: $(TARGET)
$(TARGET): src/coordinator.cc
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $<
clean: ; rm -f $(TARGET)
