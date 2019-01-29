#include <cstddef>
#include <iostream>
#include <optional>
#include <random>
#include <sqlite3.h>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

namespace sqlite
{
template <typename T = std::byte>
struct blob
{
    const T* data;
    const int size;

    constexpr blob(const T* data, int size) noexcept
        : data(data)
        , size(size)
    {}
};

template <typename>
struct loader;

template <>
struct loader<std::int32_t>
{
    static std::int32_t get(sqlite3_stmt* stmt, int index)
    {
        return sqlite3_column_int(stmt, index);
    }
};

template <>
struct loader<std::int64_t>
{
    static std::int64_t get(sqlite3_stmt* stmt, int index)
    {
        return sqlite3_column_int64(stmt, index);
    }
};

template <>
struct loader<double>
{
    static double get(sqlite3_stmt* stmt, int index)
    {
        return sqlite3_column_double(stmt, index);
    }
};

template <>
struct loader<const char*>
{
    static const char* get(sqlite3_stmt* stmt, int index)
    {
        return reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
    }
};

template <>
struct loader<std::string>
{
    static std::string get(sqlite3_stmt* stmt, int index)
    {
        int length = sqlite3_column_bytes(stmt, index);
        auto str_ptr = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
        return std::string(str_ptr, length);
    }
};

template <>
struct loader<std::string_view>
{
    static std::string_view get(sqlite3_stmt* stmt, int index)
    {
        int length = sqlite3_column_bytes(stmt, index);
        auto str_ptr = reinterpret_cast<const char*>(sqlite3_column_text(stmt, index));
        return std::string_view(str_ptr, length);
    }
};

template <typename T>
struct loader<std::optional<T>>
{
    static std::optional<T> get(sqlite3_stmt* stmt, int index)
    {
        if (sqlite3_column_type(stmt, index) == SQLITE_NULL)
            return {};
        return loader<T>::get(stmt, index);
    }
};

template <typename T>
struct loader<blob<T>>
{
    static blob<T> get(sqlite3_stmt* stmt, int index)
    {
        int length = sqlite3_column_bytes(stmt, index);
        if constexpr (!std::is_same_v<T, void>)
            if (length % sizeof(T))
                throw std::logic_error("size of blob is not divisible by size of type");

        auto data_ptr = reinterpret_cast<const T*>(sqlite3_column_blob(stmt, index));

        if constexpr (std::is_same_v<T, void>)
            return blob<T>(data_ptr, length);
        else
            return blob<T>(data_ptr, length / sizeof(T));
    }
};

template <typename T>
struct loader<std::vector<T>>
{
    static std::vector<T> get(sqlite3_stmt* stmt, int index)
    {
        int length = sqlite3_column_bytes(stmt, index);
        if (length % sizeof(T))
            throw std::logic_error("size of blob is not divisible by size of type");
        auto begin = reinterpret_cast<const char*>(sqlite3_column_blob(stmt, index));
        auto end = begin + length;

        return std::vector<T>(reinterpret_cast<const T*>(begin), reinterpret_cast<const T*>(end));
    }
};

class database;

enum class openflags
{
    readonly = SQLITE_OPEN_READONLY,
    readwrite = SQLITE_OPEN_READWRITE,
    create = SQLITE_OPEN_CREATE,
    nomutex = SQLITE_OPEN_NOMUTEX,
    fullmutex = SQLITE_OPEN_FULLMUTEX,
    sharedcache = SQLITE_OPEN_SHAREDCACHE,
    privatecach = SQLITE_OPEN_PRIVATECACHE,
    uri = SQLITE_OPEN_URI
};

constexpr openflags operator|(const openflags& a, const openflags& b) noexcept
{
    return static_cast<openflags>(static_cast<unsigned int>(a) | static_cast<unsigned int>(b));
}

class statement
{
    friend class database;

    statement(sqlite3* db, const std::string_view sql)
    {
        if (sqlite3_prepare_v2(db, sql.data(), sql.length(), &this->stmt, nullptr))
            throw std::exception();
    }

public:
    constexpr statement() noexcept
        : stmt(nullptr)
    {}
    constexpr statement(const statement&) noexcept = delete;
    constexpr statement(statement&& other) noexcept
        : stmt(other.stmt)
    {
        other.stmt = nullptr;
    }

    ~statement()
    {
        sqlite3_finalize(this->stmt);
    }

    statement& operator=(const statement&) = delete;

    statement& operator=(statement&& other)
    {
        sqlite3_finalize(this->stmt);
        this->stmt = other.stmt;
        other.stmt = nullptr;
        return *this;
    }

    void reset() const
    {
        sqlite3_reset(this->stmt);
    }

    void bind(int index, std::int32_t item) const
    {
        sqlite3_bind_int(this->stmt, index + 1, item);
    }

    void bind(int index, std::int64_t item) const
    {
        sqlite3_bind_int64(this->stmt, index + 1, item);
    }

    void bind(int index, double item) const
    {
        sqlite3_bind_double(this->stmt, index + 1, item);
    }

    void bind(int index, const std::string_view item) const
    {
        sqlite3_bind_text(this->stmt, index + 1, item.data(), item.length(), nullptr);
    }

    void bind(int index, std::nullptr_t) const
    {
        sqlite3_bind_null(this->stmt, index + 1);
    }

    void bind_multiple() const {}

    template <typename... Args, std::size_t... I>
    void bind_multiple_impl(std::index_sequence<I...>, Args&&... args) const
    {
        (bind(I, args), ...);
    }

    template <typename... Args>
    void bind_multiple(Args&&... args) const
    {
        bind_multiple_impl(std::index_sequence_for<Args...> {}, std::forward<Args>(args)...);
    }

    template <int Index, typename T>
    T get() const
    {
        return loader<T>::get(this->stmt, Index);
    }

    template <typename... Args, std::size_t... I>
    std::tuple<Args...> get_all_impl(std::tuple<Args...>&&, std::index_sequence<I...>) const
    {
        return std::tuple<Args...>(loader<Args>::get(this->stmt, I)...);
    }

    template <typename... Args>
    std::tuple<Args...> get_all() const
    {
        return get_all_impl(std::tuple<Args...> {}, std::index_sequence_for<Args...> {});
    }

    bool step() const
    {
        int result = sqlite3_step(this->stmt);
        if (result == SQLITE_ROW)
            return true;
        else if (result == SQLITE_DONE)
            return false;
        else
            throw std::exception();
    }

    sqlite3_stmt* handle() const
    {
        return stmt;
    }

private:
    sqlite3_stmt* stmt;
};

class database
{
public:
    database() = delete;
    database(const std::string_view filename, openflags flags = openflags::readwrite | openflags::create)
    {
        if (sqlite3_open_v2(filename.data(), &this->db, static_cast<int>(flags), nullptr))
            throw std::invalid_argument("database file not found");
    }

    database(const database&) = delete;

    constexpr database(database&& other) noexcept
        : db(other.db)
        , transaction_id(other.transaction_id)
    {
        other.db = nullptr;
        other.transaction_id = 0;
    }

    ~database()
    {
        sqlite3_close_v2(this->db);
    }

    database& operator=(const database&) = delete;

    statement prepare(const std::string_view sql) const
    {
        return statement(this->db, sql);
    }

    template <typename... Args>
    statement execute(const std::string_view sql, Args... args) const
    {
        statement stmt = this->prepare(sql);
        stmt.reset();
        stmt.bind_multiple(std::forward<Args>(args)...);
        return stmt;
    }

    template <typename F>
    void atomic(F&& func)
    {
        std::string transaction_id = std::to_string(this->transaction_id++);
        std::string begin_sql = "savepoint s" + transaction_id;
        std::string rollback_sql = "rollback transaction to savepoint s" + transaction_id;
        std::string commit_sql = "release savepoint s" + transaction_id;

        this->execute(begin_sql);
        try
        {
            func();
        }
        catch (...)
        {
            this->execute(rollback_sql);
            throw;
        }

        this->execute(commit_sql);
    }

    sqlite3* handle() const
    {
        return db;
    }

private:
    sqlite3* db;
    int transaction_id = 0;
};

}  // namespace sqlite
