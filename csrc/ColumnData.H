#ifndef INC_ColumnData_H_
#define INC_ColumnData_H_

// Column-oriented typed data storage for DBD files.
// Instead of storing rows of doubles (like Data.C), stores one typed vector
// per sensor column with native dtypes (int8, int16, float, double).
// This allows zero-copy transfer to numpy arrays via pybind11.

#include <cstdint>
#include <iosfwd>
#include <string>
#include <variant>
#include <vector>

class KnownBytes;
class Sensors;

using TypedColumn = std::variant<
    std::vector<int8_t>,
    std::vector<int16_t>,
    std::vector<float>,
    std::vector<double>>;

struct SensorInfo {
    std::string name;
    std::string units;
    int size; // 1, 2, 4, or 8
};

struct ColumnDataResult {
    std::vector<TypedColumn> columns; // One per output sensor
    std::vector<SensorInfo> sensor_info;
    size_t n_records;
};

ColumnDataResult read_columns(std::istream& is,
                              const KnownBytes& kb,
                              const Sensors& sensors,
                              bool qRepair,
                              size_t nBytes);

#endif // INC_ColumnData_H_
