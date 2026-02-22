// Column-oriented typed data parser for DBD files.
// Adapted from Data.C but stores native-typed columns instead of double rows.

#include "ColumnData.H"
#include "KnownBytes.H"
#include "Sensors.H"
#include "MyException.H"
#include "Logger.H"
#include <iostream>
#include <sstream>
#include <cmath>
#include <cstring>

ColumnDataResult read_columns(std::istream& is,
                              const KnownBytes& kb,
                              const Sensors& sensors,
                              bool qRepair,
                              size_t nBytes)
{
    const size_t nSensors = sensors.size();
    const size_t nHeader = (nSensors + 3) / 4;
    std::vector<int8_t> bits(nHeader);

    // Build sensor info and determine which sensors to keep
    // nToStore is the number of output columns (sensors marked keep)
    const size_t nToStore = sensors.nToStore();

    // Map from sensor index -> output column index (or -1 if not kept)
    // Also track sensor sizes for the output columns
    std::vector<int> outIndex(nSensors, -1);
    std::vector<SensorInfo> sensorInfo;
    sensorInfo.reserve(nToStore);

    {
        for (size_t i = 0; i < nSensors; ++i) {
            const Sensor& s = sensors[i];
            if (s.qKeep()) {
                outIndex[i] = s.index();
                // Only add to sensorInfo if this is a new output index
                if (static_cast<size_t>(s.index()) >= sensorInfo.size()) {
                    sensorInfo.resize(s.index() + 1);
                }
                sensorInfo[s.index()] = {s.name(), s.units(), s.size()};
            }
        }
    }

    const size_t nOut = sensorInfo.size();

    // Initial capacity estimate
    const size_t initCapacity = std::max<size_t>(256, 2 * nBytes / (nHeader + 1) + 1);

    // Create typed columns based on sensor sizes
    std::vector<TypedColumn> columns(nOut);
    for (size_t i = 0; i < nOut; ++i) {
        switch (sensorInfo[i].size) {
            case 1: columns[i] = std::vector<int8_t>(initCapacity, FILL_INT8); break;
            case 2: columns[i] = std::vector<int16_t>(initCapacity, FILL_INT16); break;
            case 4: columns[i] = std::vector<float>(initCapacity, NAN); break;
            case 8: columns[i] = std::vector<double>(initCapacity, NAN); break;
            default: columns[i] = std::vector<double>(initCapacity, NAN); break;
        }
    }

    // Previous values per output column — initialized to fill values
    std::vector<TypedColumn> prevValues(nOut);
    for (size_t i = 0; i < nOut; ++i) {
        switch (sensorInfo[i].size) {
            case 1: prevValues[i] = std::vector<int8_t>(1, FILL_INT8); break;
            case 2: prevValues[i] = std::vector<int16_t>(1, FILL_INT16); break;
            case 4: prevValues[i] = std::vector<float>(1, NAN); break;
            case 8: prevValues[i] = std::vector<double>(1, NAN); break;
            default: prevValues[i] = std::vector<double>(1, NAN); break;
        }
    }

    size_t nRows = 0;

    // Wrap the parsing loop in try-catch to retain partial results on I/O errors.
    // This matches C++ dbd2netCDF behavior which catches exceptions in the
    // file-processing loop and retains all records read before the error.
    try {
    while (true) {
        int8_t tag;
        if (!is.read(reinterpret_cast<char*>(&tag), 1)) {
            break; // EOF
        }

        if (tag == 'X') {
            break; // End-of-data tag
        }

        if (tag != 'd') {
            // Not a data tag - try to find the next 'd'
            const auto pos = is.tellg();
            bool qContinue = false;
            while (true) {
                int8_t c;
                if (!is.read(reinterpret_cast<char*>(&c), 1)) break;
                if (c == 'd') {
                    qContinue = true;
                    break;
                }
            }
            if (!qRepair || !qContinue) {
                break; // Stop parsing, retain what we have
            }
        }

        if (!is.read(reinterpret_cast<char*>(bits.data()), nHeader)) {
            break; // EOF reading header bits, retain what we have
        }

        bool qKeep = false;

        for (size_t i = 0; i < nSensors; ++i) {
            const size_t offIndex = i >> 2;
            const size_t offBits = 6 - ((i & 0x3) << 1);
            const unsigned int code = (bits[offIndex] >> offBits) & 0x03;

            if (code == 1) { // Repeat previous value
                const Sensor& sensor = sensors[i];
                qKeep |= sensor.qCriteria();
                const int oi = outIndex[i];
                if (oi >= 0) {
                    // Copy previous value into current row
                    std::visit([nRows, oi](auto& col_vec, const auto& prev_vec) {
                        using T = typename std::decay_t<decltype(col_vec)>::value_type;
                        using PT = typename std::decay_t<decltype(prev_vec)>::value_type;
                        if constexpr (std::is_same_v<T, PT>) {
                            if (nRows >= col_vec.size()) {
                                if constexpr (std::is_same_v<T, int8_t>)
                                    col_vec.resize(col_vec.size() * 2, FILL_INT8);
                                else if constexpr (std::is_same_v<T, int16_t>)
                                    col_vec.resize(col_vec.size() * 2, FILL_INT16);
                                else
                                    col_vec.resize(col_vec.size() * 2, NAN);
                            }
                            col_vec[nRows] = prev_vec[0];
                        }
                    }, columns[oi], prevValues[oi]);
                }
            } else if (code == 2) { // New value
                const Sensor& sensor = sensors[i];
                qKeep |= sensor.qCriteria();
                const int oi = outIndex[i];

                // Must read the value regardless of whether we keep it
                switch (sensor.size()) {
                    case 1: {
                        int8_t val = kb.read8(is);
                        if (oi >= 0) {
                            auto& vec = std::get<std::vector<int8_t>>(columns[oi]);
                            if (nRows >= vec.size()) vec.resize(vec.size() * 2, FILL_INT8);
                            vec[nRows] = val;
                            std::get<std::vector<int8_t>>(prevValues[oi])[0] = val;
                        }
                        break;
                    }
                    case 2: {
                        int16_t val = kb.read16(is);
                        if (oi >= 0) {
                            auto& vec = std::get<std::vector<int16_t>>(columns[oi]);
                            if (nRows >= vec.size()) vec.resize(vec.size() * 2, FILL_INT16);
                            vec[nRows] = val;
                            std::get<std::vector<int16_t>>(prevValues[oi])[0] = val;
                        }
                        break;
                    }
                    case 4: {
                        float val = kb.read32(is);
                        if (std::isinf(val)) val = NAN;
                        if (oi >= 0) {
                            auto& vec = std::get<std::vector<float>>(columns[oi]);
                            if (nRows >= vec.size()) vec.resize(vec.size() * 2, NAN);
                            vec[nRows] = val;
                            std::get<std::vector<float>>(prevValues[oi])[0] = val;
                        }
                        break;
                    }
                    case 8: {
                        double val = kb.read64(is);
                        if (std::isinf(val)) val = NAN;
                        if (oi >= 0) {
                            auto& vec = std::get<std::vector<double>>(columns[oi]);
                            if (nRows >= vec.size()) vec.resize(vec.size() * 2, NAN);
                            vec[nRows] = val;
                            std::get<std::vector<double>>(prevValues[oi])[0] = val;
                        }
                        break;
                    }
                    default: {
                        std::ostringstream oss;
                        oss << "Unknown sensor size " << sensor.size()
                            << " for sensor " << sensor.name();
                        throw MyException(oss.str());
                    }
                }
            }
            // code == 0: absent, do nothing (fill value already in column)
        }

        if (qKeep) {
            ++nRows;
        }
    }
    } catch (const std::exception&) {
        // Retain fully-parsed records; discard the partially-read record.
        // C++ dbd2netCDF resizes mData to nRows, discarding the partial row.
    }

    // Trim columns to actual size
    for (size_t i = 0; i < nOut; ++i) {
        std::visit([nRows](auto& vec) {
            vec.resize(nRows);
            vec.shrink_to_fit();
        }, columns[i]);
    }

    return {std::move(columns), std::move(sensorInfo), nRows};
}
