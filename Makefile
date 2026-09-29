CXX      ?= g++
CXXFLAGS ?= -std=c++20 -O2 -DNDEBUG -Wall -Wextra -Iinclude
SAN      := -std=c++20 -O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer -Iinclude
HDRS     := $(wildcard include/tickscope/*.hpp)

.PHONY: all test asan tsan bench clean
all: build/bench build/tests

build/%: src/%.cpp $(HDRS)
	@mkdir -p build
	$(CXX) $(CXXFLAGS) $< -o $@ -pthread

test: build/tests
	./build/tests

asan: $(HDRS) src/tests.cpp
	@mkdir -p build
	$(CXX) $(SAN) src/tests.cpp -o build/tests_asan -pthread && ./build/tests_asan

tsan: $(HDRS) src/tests.cpp
	@mkdir -p build
	$(CXX) -std=c++20 -O1 -g -fsanitize=thread -Iinclude src/tests.cpp -o build/tests_tsan -pthread && ./build/tests_tsan

bench: build/bench
	./build/bench

clean:
	rm -rf build
