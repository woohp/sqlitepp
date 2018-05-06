#include "sqlitepp.hpp"


int main()
{
    database db("test.db");
    db.atomic([&]()
    {
        cout << "1\n";
        auto stmt = db.execute("select id, id * 2 from foo");
        cout << "2\n";
        while (stmt.step())
        {
            auto row = stmt.get_all<optional<int>, string_view>();
            auto [id, id2] = row;
            if (id)
                cout << *id << ' ' << id2 << endl;
            else
                cout << "null\n";
        }
    });

    return 0;
}
