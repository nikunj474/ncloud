CXX      := g++
CXXFLAGS := -std=c++17 -O2 -Wall -Wextra -pthread -Isrc
TARGET   := smtp_server
SRC      := src/smtp_server.cc
LDLIBS   := -lresolv

all: $(TARGET)

$(TARGET): $(SRC)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -f $(TARGET)

.PHONY: all clean
