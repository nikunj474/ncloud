CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread
TARGET   := frontend_lb
SRC      := src/load_balancer.cc

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^

clean:
	rm -f $(TARGET)

.PHONY: all clean
