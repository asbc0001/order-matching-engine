// book_dump_test.cpp - Checks for deterministic final book dumps.

#include <array>
#include <cstdio>
#include <cstring>

#include "orderbook/book_dump.hpp"
#include "orderbook/config.hpp"

namespace {

using TestBook = ob::Book<ob::config::kFuzz.num_levels, 8>;

bool fail(const char* message) {
    std::fprintf(stderr, "%s\n", message);
    return false;
}

bool read_dump(TestBook& book, char* buffer, std::size_t capacity) {
    std::FILE* file = std::tmpfile();
    if (file == nullptr) {
        return false;
    }

    const bool dumped = ob::dump_book(book, file);
    const bool rewound = std::fflush(file) == 0 && std::fseek(file, 0, SEEK_SET) == 0;
    const std::size_t used = rewound ? std::fread(buffer, 1, capacity - 1, file) : 0;
    buffer[used] = '\0';
    const bool read_ok = std::ferror(file) == 0;
    std::fclose(file);
    return dumped && rewound && read_ok;
}

bool check_dump_includes_best_levels_and_fifo_orders() {
    TestBook book;
    const auto first_bid = book.insert(ob::Side::Bid, 10, 30, 101);
    const auto second_bid = book.insert(ob::Side::Bid, 10, 40, 102);
    const auto ask = book.insert(ob::Side::Ask, 20, 50, 201);
    if (!first_bid || !second_bid || !ask) {
        return fail("Book setup failed");
    }

    char buffer[512]{};
    constexpr char expected[] =
        "BEST bid=10 ask=20\n"
        "LEVEL price=10 count=2 qty=70\n"
        "  ORDER handle=4294967296 client_seq=101 remaining=30 side=Bid\n"
        "  ORDER handle=4294967297 client_seq=102 remaining=40 side=Bid\n"
        "LEVEL price=20 count=1 qty=50\n"
        "  ORDER handle=4294967298 client_seq=201 remaining=50 side=Ask\n";

    return read_dump(book, buffer, sizeof(buffer)) && std::strcmp(buffer, expected) == 0
               ? true
               : fail("Book dump text mismatch");
}

}  // namespace

int main() {
    using Check = bool (*)();
    constexpr std::array<Check, 1> checks{
        check_dump_includes_best_levels_and_fifo_orders,
    };

    for (Check check : checks) {
        if (!check()) {
            return 1;
        }
    }

    std::printf("book_dump_test OK\n");
    return 0;
}
