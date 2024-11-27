#include <iostream>
#include <memory>
#include <vector>

using namespace std;

class Dummy {
    int value;

  public:
    Dummy(int d) : value(d) { /* copy constructor */ cout << "VALUE" << endl; }
    // Dummy(const Dummy& d) { /* copy constructor */ cout << "COPY" << endl; }
    Dummy(Dummy&& d) noexcept { /* move constructor */ cout << "MOVE" << endl; }
};

// int main() {
//     vector<Dummy> d;

//     d.push_back(3);
//     d.push_back(3);
//     // d.push_back(3);
//     // d.push_back(3);
//     // d.push_back(3);
//     // d.push_back(3);
//     // d.push_back(3);
//     // d.push_back(3);
// }

int main() {

    unique_ptr<Dummy> u;
    vector<Dummy> v;

    auto u2 = std::move(u);

    v.push_back(move(*u2));

    volatile int dummy = 0;
}