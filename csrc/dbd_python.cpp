// pybind11 binding module for the dbd2netCDF C++ parser
// Exposes read_dbd_file() and read_dbd_files() to Python

#include <pybind11/pybind11.h>
#include <pybind11/numpy.h>
#include <pybind11/stl.h>

#include "Header.H"
#include "Sensors.H"
#include "SensorsMap.H"
#include "KnownBytes.H"
#include "Decompress.H"
#include "ColumnData.H"
#include "MyException.H"

#include <fstream>
#include <sstream>
#include <filesystem>
#include <algorithm>
#include <cmath>

namespace py = pybind11;
namespace fs = std::filesystem;

namespace {

// Convert a TypedColumn to a numpy array with zero-copy via capsule
py::array typed_column_to_numpy(TypedColumn&& col) {
    return std::visit([](auto&& vec) -> py::array {
        using VecType = std::decay_t<decltype(vec)>;
        using T = typename VecType::value_type;

        // Move the vector to the heap so the capsule can own it
        auto* heap_vec = new VecType(std::move(vec));
        auto capsule = py::capsule(heap_vec, [](void* p) {
            delete static_cast<VecType*>(p);
        });

        return py::array_t<T>(
            {static_cast<py::ssize_t>(heap_vec->size())},  // shape
            {sizeof(T)},                                     // strides
            heap_vec->data(),                                // data pointer
            capsule                                          // capsule owns the data
        );
    }, std::move(col));
}

// Extract header fields into a Python dict
py::dict header_to_dict(const Header& hdr) {
    py::dict d;
    d["mission_name"] = hdr.find("mission_name");
    d["fileopen_time"] = hdr.find("fileopen_time");
    d["encoding_version"] = hdr.find("encoding_ver");
    d["full_filename"] = hdr.find("full_filename");
    d["sensor_list_crc"] = hdr.find("sensor_list_crc");
    d["the8x3_filename"] = hdr.find("the8x3_filename");
    d["filename_extension"] = hdr.find("filename_extension");
    return d;
}

// Read a single DBD file and return column-oriented data
py::dict read_dbd_file_impl(
    const std::string& filename,
    const std::string& cache_dir,
    const std::vector<std::string>& to_keep,
    const std::vector<std::string>& criteria,
    bool skip_first_record,
    bool repair)
{
    // Open file with decompression support
    DecompressTWR is(filename, qCompressed(filename));
    if (!is) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    // Read header
    Header hdr(is, filename.c_str());
    if (hdr.empty()) {
        throw std::runtime_error("Empty or invalid header in " + filename);
    }

    // Read or load sensors
    Sensors sensors(is, hdr);

    if (sensors.empty() && !cache_dir.empty()) {
        // Factored file - load from cache
        sensors.load(cache_dir, hdr);
    } else if (!sensors.empty() && !cache_dir.empty()) {
        // Unfactored file - dump to cache for future use
        sensors.dump(cache_dir);
    }

    if (sensors.empty()) {
        throw std::runtime_error("No sensors found for " + filename);
    }

    // Apply keep/criteria filters
    if (!to_keep.empty()) {
        Sensors::tNames keepNames(to_keep.begin(), to_keep.end());
        sensors.qKeep(keepNames);
    }
    if (!criteria.empty()) {
        Sensors::tNames critNames(criteria.begin(), criteria.end());
        sensors.qCriteria(critNames);
    }

    // Read known bytes for endianness detection
    KnownBytes kb(is);

    // Estimate remaining bytes (for initial allocation)
    // For compressed streams tellg may not be accurate, use a reasonable default
    size_t nBytes = 1024 * 1024; // Default 1MB estimate

    // Read column-oriented data
    ColumnDataResult result = read_columns(is, kb, sensors, repair, nBytes);

    // Handle skip_first_record: trim first record if requested and data exists
    size_t start = 0;
    size_t n_records = result.n_records;
    if (skip_first_record && n_records > 0) {
        start = 1;
        n_records -= 1;
    }

    // Convert to Python objects
    py::list columns;
    py::list sensor_names;
    py::list sensor_units;
    py::list sensor_sizes;

    for (size_t i = 0; i < result.columns.size(); ++i) {
        if (start > 0 && result.n_records > 0) {
            // Need to slice: skip first record
            py::array full_arr = typed_column_to_numpy(std::move(result.columns[i]));
            // Slice from start to end
            columns.append(full_arr[py::slice(py::int_(start), py::none(), py::none())]);
        } else {
            columns.append(typed_column_to_numpy(std::move(result.columns[i])));
        }
        sensor_names.append(result.sensor_info[i].name);
        sensor_units.append(result.sensor_info[i].units);
        sensor_sizes.append(result.sensor_info[i].size);
    }

    py::dict out;
    out["columns"] = columns;
    out["sensor_names"] = sensor_names;
    out["sensor_units"] = sensor_units;
    out["sensor_sizes"] = sensor_sizes;
    out["n_records"] = n_records;
    out["header"] = header_to_dict(hdr);
    out["filename"] = filename;

    return out;
}

// Read multiple DBD files using SensorsMap for union sensor handling
py::dict read_dbd_files_impl(
    const std::vector<std::string>& filenames,
    const std::string& cache_dir,
    const std::vector<std::string>& to_keep,
    const std::vector<std::string>& criteria,
    const std::vector<std::string>& skip_missions,
    const std::vector<std::string>& keep_missions,
    bool skip_first_record,
    bool repair)
{
    if (filenames.empty()) {
        py::dict out;
        out["columns"] = py::list();
        out["sensor_names"] = py::list();
        out["sensor_units"] = py::list();
        out["sensor_sizes"] = py::list();
        out["n_records"] = 0;
        out["n_files"] = 0;
        return out;
    }

    // Sort filenames
    std::vector<std::string> sorted_files(filenames);
    std::sort(sorted_files.begin(), sorted_files.end());

    // Build mission filter sets
    Header::tMissions skipSet, keepSet;
    for (const auto& m : skip_missions) Header::addMission(m, skipSet);
    for (const auto& m : keep_missions) Header::addMission(m, keepSet);

    // Two-pass approach:
    // Pass 1: Scan all headers, build SensorsMap (union of all sensor sets)
    SensorsMap smap(cache_dir);

    struct FileEntry {
        std::string filename;
        // We re-open files in pass 2
    };
    std::vector<FileEntry> valid_files;

    for (const auto& fn : sorted_files) {
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;

            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;
            if (!hdr.qProcessMission(skipSet, keepSet)) continue;

            smap.insert(is, hdr, true);
            valid_files.push_back({fn});
        } catch (const std::exception&) {
            // Skip files that fail
            continue;
        }
    }

    if (valid_files.empty()) {
        py::dict out;
        out["columns"] = py::list();
        out["sensor_names"] = py::list();
        out["sensor_units"] = py::list();
        out["sensor_sizes"] = py::list();
        out["n_records"] = 0;
        out["n_files"] = 0;
        return out;
    }

    // Apply keep/criteria filters
    if (!to_keep.empty()) {
        Sensors::tNames keepNames(to_keep.begin(), to_keep.end());
        smap.qKeep(keepNames);
    }
    if (!criteria.empty()) {
        Sensors::tNames critNames(criteria.begin(), criteria.end());
        smap.qCriteria(critNames);
    }

    // Set up union sensor indices
    smap.setUpForData();
    const Sensors& allSensors = smap.allSensors();

    // Build sensor info from union
    const size_t nOut = allSensors.nToStore();
    std::vector<SensorInfo> unionInfo(nOut);
    for (size_t i = 0; i < allSensors.size(); ++i) {
        const Sensor& s = allSensors[i];
        if (s.qKeep()) {
            size_t idx = static_cast<size_t>(s.index());
            if (idx < nOut) {
                unionInfo[idx] = {s.name(), s.units(), s.size()};
            }
        }
    }

    // Pass 2: Read data from each file, accumulate into union columns
    // First pass to count total records
    struct FileData {
        ColumnDataResult result;
        Sensors::tMap sensorMap;
    };
    std::vector<FileData> allData;
    size_t totalRecords = 0;

    for (size_t fi = 0; fi < valid_files.size(); ++fi) {
        const auto& fn = valid_files[fi].filename;
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;

            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;

            const Sensors& fileSensors = smap.find(hdr);
            KnownBytes kb(is);

            ColumnDataResult result = read_columns(is, kb, fileSensors, repair, 1024 * 1024);

            size_t n = result.n_records;
            // skip_first_record: skip first record from all files except the first contributing one
            if (skip_first_record && !allData.empty() && n > 0) {
                n -= 1;
            }
            totalRecords += n;

            // Build map from this file's sensors to union indices
            // Not needed here - fileSensors already has correct indices from setUpForData
            allData.push_back({std::move(result), {}});
        } catch (const std::exception&) {
            continue;
        }
    }

    // Allocate union columns with fill=0
    std::vector<TypedColumn> unionColumns(nOut);
    for (size_t i = 0; i < nOut; ++i) {
        switch (unionInfo[i].size) {
            case 1: unionColumns[i] = std::vector<int8_t>(totalRecords, 0); break;
            case 2: unionColumns[i] = std::vector<int16_t>(totalRecords, 0); break;
            case 4: unionColumns[i] = std::vector<float>(totalRecords, NAN); break;
            case 8: unionColumns[i] = std::vector<double>(totalRecords, NAN); break;
            default: unionColumns[i] = std::vector<double>(totalRecords, NAN); break;
        }
    }

    // Copy data from each file into union columns
    size_t offset = 0;
    for (size_t fi = 0; fi < allData.size(); ++fi) {
        auto& fd = allData[fi];
        size_t n = fd.result.n_records;
        size_t start = 0;
        if (skip_first_record && fi > 0 && n > 0) {
            start = 1;
            n -= 1;
        }
        if (n == 0) continue;

        // fd.result.sensor_info has the per-file sensor info
        // fd.result.columns are indexed by the per-file sensor order
        // But since we used smap.find() which sets indices via setUpForData(),
        // the indices in the file's sensors match the union layout.
        // However, the columns in the result are indexed 0..nFileSensors,
        // and each column's position in the union is given by sensor_info's
        // corresponding sensor index.

        // We need to map: for each column i in result, sensor_info[i].name
        // tells us which union column to write to.
        for (size_t ci = 0; ci < fd.result.columns.size(); ++ci) {
            // Find the union column index by name
            const std::string& name = fd.result.sensor_info[ci].name;
            int unionIdx = -1;
            for (size_t ui = 0; ui < nOut; ++ui) {
                if (unionInfo[ui].name == name) {
                    unionIdx = static_cast<int>(ui);
                    break;
                }
            }
            if (unionIdx < 0) continue;

            // Copy data from source column to union column
            std::visit([offset, start, n, unionIdx, &unionColumns](const auto& src_vec) {
                using T = typename std::decay_t<decltype(src_vec)>::value_type;
                auto& dst_vec = std::get<std::vector<T>>(unionColumns[unionIdx]);
                for (size_t r = 0; r < n; ++r) {
                    dst_vec[offset + r] = src_vec[start + r];
                }
            }, fd.result.columns[ci]);
        }
        offset += n;
    }

    // Convert to Python
    py::list columns;
    py::list sensor_names;
    py::list sensor_units;
    py::list sensor_sizes;

    for (size_t i = 0; i < nOut; ++i) {
        columns.append(typed_column_to_numpy(std::move(unionColumns[i])));
        sensor_names.append(unionInfo[i].name);
        sensor_units.append(unionInfo[i].units);
        sensor_sizes.append(unionInfo[i].size);
    }

    py::dict out;
    out["columns"] = columns;
    out["sensor_names"] = sensor_names;
    out["sensor_units"] = sensor_units;
    out["sensor_sizes"] = sensor_sizes;
    out["n_records"] = totalRecords;
    out["n_files"] = valid_files.size();

    return out;
}

} // anonymous namespace


PYBIND11_MODULE(_dbd_cpp, m) {
    m.doc() = "C++ backend for reading Dinkum Binary Data (DBD) files";

    m.def("read_dbd_file",
        [](const std::string& filename,
           const std::string& cache_dir,
           const std::vector<std::string>& to_keep,
           const std::vector<std::string>& criteria,
           bool skip_first_record,
           bool repair) -> py::dict {
            // Release GIL during file I/O and parsing
            py::dict result;
            {
                py::gil_scoped_release release;
                // Can't create Python objects without GIL, so we need to
                // do it differently - release around parsing only
            }
            // Actually, we need the GIL for creating numpy arrays.
            // Release it around the C++ parsing, then create arrays.
            return read_dbd_file_impl(filename, cache_dir, to_keep, criteria,
                                      skip_first_record, repair);
        },
        py::arg("filename"),
        py::arg("cache_dir") = "",
        py::arg("to_keep") = std::vector<std::string>(),
        py::arg("criteria") = std::vector<std::string>(),
        py::arg("skip_first_record") = true,
        py::arg("repair") = false,
        "Read a single DBD file and return column-oriented data.\n\n"
        "Returns a dict with keys: columns, sensor_names, sensor_units, "
        "sensor_sizes, n_records, header, filename"
    );

    m.def("read_dbd_files",
        &read_dbd_files_impl,
        py::arg("filenames"),
        py::arg("cache_dir") = "",
        py::arg("to_keep") = std::vector<std::string>(),
        py::arg("criteria") = std::vector<std::string>(),
        py::arg("skip_missions") = std::vector<std::string>(),
        py::arg("keep_missions") = std::vector<std::string>(),
        py::arg("skip_first_record") = true,
        py::arg("repair") = false,
        "Read multiple DBD files with sensor union and return concatenated data.\n\n"
        "Returns a dict with keys: columns, sensor_names, sensor_units, "
        "sensor_sizes, n_records, n_files"
    );
}
