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
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace py = pybind11;
namespace fs = std::filesystem;

namespace {

// ── Pure-C++ result types (no Python objects) ──────────────────────────

struct HeaderFields {
    std::string mission_name;
    std::string fileopen_time;
    std::string encoding_ver;
    std::string full_filename;
    std::string sensor_list_crc;
    std::string the8x3_filename;
    std::string filename_extension;
};

struct SingleFileResult {
    std::vector<TypedColumn> columns;
    std::vector<SensorInfo> sensor_info;
    size_t n_records;
    size_t start;  // 0 or 1, for skip_first_record slicing
    HeaderFields header;
    std::string filename;
};

struct MultiFileResult {
    std::vector<TypedColumn> columns;
    std::vector<SensorInfo> sensor_info;
    size_t n_records;
    size_t n_files;
};

struct SensorListResult {
    std::vector<SensorInfo> sensor_info;
    size_t n_files;
};

struct FileHeaderInfo {
    std::string filename;
    std::string mission_name;
    std::string sensor_list_crc;
};

struct HeaderScanResult {
    std::vector<FileHeaderInfo> file_headers;
};

// ── Pure-C++ parsing (called with GIL released) ────────────────────────

HeaderFields extract_header_fields(const Header& hdr) {
    return {
        hdr.find("mission_name"),
        hdr.find("fileopen_time"),
        hdr.find("encoding_ver"),
        hdr.find("full_filename"),
        hdr.find("sensor_list_crc"),
        hdr.find("the8x3_filename"),
        hdr.find("filename_extension"),
    };
}

SingleFileResult parse_single_file(
    const std::string& filename,
    const std::string& cache_dir,
    const std::vector<std::string>& to_keep,
    const std::vector<std::string>& criteria,
    bool skip_first_record,
    bool repair)
{
    DecompressTWR is(filename, qCompressed(filename));
    if (!is) {
        throw std::runtime_error("Cannot open file: " + filename);
    }

    Header hdr(is, filename.c_str());
    if (hdr.empty()) {
        throw std::runtime_error("Empty or invalid header in " + filename);
    }

    Sensors sensors(is, hdr);

    if (sensors.empty() && !cache_dir.empty()) {
        sensors.load(cache_dir, hdr);
    } else if (!sensors.empty() && !cache_dir.empty()) {
        sensors.dump(cache_dir);
    }

    if (sensors.empty()) {
        throw std::runtime_error("No sensors found for " + filename);
    }

    if (!to_keep.empty()) {
        Sensors::tNames keepNames(to_keep.begin(), to_keep.end());
        sensors.qKeep(keepNames);
    }
    if (!criteria.empty()) {
        Sensors::tNames critNames(criteria.begin(), criteria.end());
        sensors.qCriteria(critNames);
    }

    KnownBytes kb(is);
    size_t nBytes = 1024 * 1024;
    ColumnDataResult result = read_columns(is, kb, sensors, repair, nBytes);

    size_t start = 0;
    size_t n_records = result.n_records;
    if (skip_first_record && n_records > 0) {
        start = 1;
        n_records -= 1;
    }

    return {
        std::move(result.columns),
        std::move(result.sensor_info),
        n_records,
        start,
        extract_header_fields(hdr),
        filename,
    };
}

MultiFileResult parse_multiple_files(
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
        return {{}, {}, 0, 0};
    }

    std::vector<std::string> sorted_files(filenames);
    std::sort(sorted_files.begin(), sorted_files.end());

    Header::tMissions skipSet, keepSet;
    for (const auto& m : skip_missions) Header::addMission(m, skipSet);
    for (const auto& m : keep_missions) Header::addMission(m, keepSet);

    // Pass 1: scan headers, build SensorsMap
    SensorsMap smap(cache_dir);
    std::vector<std::string> valid_files;

    for (const auto& fn : sorted_files) {
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;
            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;
            if (!hdr.qProcessMission(skipSet, keepSet)) continue;
            smap.insert(is, hdr, true);
            valid_files.push_back(fn);
        } catch (const std::exception&) {
            continue;
        }
    }

    if (valid_files.empty()) {
        return {{}, {}, 0, 0};
    }

    if (!to_keep.empty()) {
        Sensors::tNames keepNames(to_keep.begin(), to_keep.end());
        smap.qKeep(keepNames);
    }
    if (!criteria.empty()) {
        Sensors::tNames critNames(criteria.begin(), criteria.end());
        smap.qCriteria(critNames);
    }

    smap.setUpForData();
    const Sensors& allSensors = smap.allSensors();

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

    // Pass 2: read data from each file
    struct FileData {
        ColumnDataResult result;
    };
    std::vector<FileData> allData;
    size_t totalRecords = 0;

    for (const auto& fn : valid_files) {
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;
            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;
            const Sensors& fileSensors = smap.find(hdr);
            // Skip inline sensor lines for unfactored files (pass 1 consumed
            // them via Sensors(is,hdr), but find() does no stream I/O).
            if (!hdr.qFactored()) {
                for (int i = hdr.nSensors(); i > 0; --i) {
                    std::string line;
                    std::getline(is, line);
                }
            }
            KnownBytes kb(is);
            ColumnDataResult result = read_columns(is, kb, fileSensors, repair, 1024 * 1024);

            size_t n = result.n_records;
            if (skip_first_record && !allData.empty() && n > 0) {
                n -= 1;
            }
            totalRecords += n;
            allData.push_back({std::move(result)});
        } catch (const std::exception&) {
            continue;
        }
    }

    // Allocate union columns
    std::vector<TypedColumn> unionColumns(nOut);
    for (size_t i = 0; i < nOut; ++i) {
        switch (unionInfo[i].size) {
            case 1: unionColumns[i] = std::vector<int8_t>(totalRecords, FILL_INT8); break;
            case 2: unionColumns[i] = std::vector<int16_t>(totalRecords, FILL_INT16); break;
            case 4: unionColumns[i] = std::vector<float>(totalRecords, NAN); break;
            case 8: unionColumns[i] = std::vector<double>(totalRecords, NAN); break;
            default: unionColumns[i] = std::vector<double>(totalRecords, NAN); break;
        }
    }

    // Build name-to-index map for O(1) lookup during merge
    std::unordered_map<std::string, int> unionNameIndex;
    for (size_t i = 0; i < nOut; ++i) {
        unionNameIndex[unionInfo[i].name] = static_cast<int>(i);
    }

    // Copy data into union columns
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

        for (size_t ci = 0; ci < fd.result.columns.size(); ++ci) {
            const std::string& name = fd.result.sensor_info[ci].name;
            auto it = unionNameIndex.find(name);
            if (it == unionNameIndex.end()) continue;
            int unionIdx = it->second;

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

    return {
        std::move(unionColumns),
        std::move(unionInfo),
        totalRecords,
        valid_files.size(),
    };
}

SensorListResult scan_sensor_list(
    const std::vector<std::string>& filenames,
    const std::string& cache_dir,
    const std::vector<std::string>& skip_missions,
    const std::vector<std::string>& keep_missions)
{
    if (filenames.empty()) {
        return {{}, 0};
    }

    std::vector<std::string> sorted_files(filenames);
    std::sort(sorted_files.begin(), sorted_files.end());

    Header::tMissions skipSet, keepSet;
    for (const auto& m : skip_missions) Header::addMission(m, skipSet);
    for (const auto& m : keep_missions) Header::addMission(m, keepSet);

    SensorsMap smap(cache_dir);
    size_t n_files = 0;

    for (const auto& fn : sorted_files) {
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;
            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;
            if (!hdr.qProcessMission(skipSet, keepSet)) continue;
            smap.insert(is, hdr, false);
            ++n_files;
        } catch (const std::exception&) {
            continue;
        }
    }

    if (n_files == 0) {
        return {{}, 0};
    }

    smap.setUpForData();
    const Sensors& allSensors = smap.allSensors();

    std::vector<SensorInfo> info;
    for (size_t i = 0; i < allSensors.size(); ++i) {
        const Sensor& s = allSensors[i];
        info.push_back({s.name(), s.units(), s.size()});
    }

    return {std::move(info), n_files};
}

HeaderScanResult scan_file_headers(
    const std::vector<std::string>& filenames,
    const std::vector<std::string>& skip_missions,
    const std::vector<std::string>& keep_missions)
{
    if (filenames.empty()) {
        return {{}};
    }

    std::vector<std::string> sorted_files(filenames);
    std::sort(sorted_files.begin(), sorted_files.end());

    Header::tMissions skipSet, keepSet;
    for (const auto& m : skip_missions) Header::addMission(m, skipSet);
    for (const auto& m : keep_missions) Header::addMission(m, keepSet);

    std::vector<FileHeaderInfo> headers;

    for (const auto& fn : sorted_files) {
        try {
            DecompressTWR is(fn, qCompressed(fn));
            if (!is) continue;
            Header hdr(is, fn.c_str());
            if (hdr.empty()) continue;
            if (!hdr.qProcessMission(skipSet, keepSet)) continue;
            headers.push_back({
                fn,
                hdr.find("mission_name"),
                hdr.find("sensor_list_crc"),
            });
        } catch (const std::exception&) {
            continue;
        }
    }

    return {std::move(headers)};
}

// ── Python conversion helpers (called with GIL held) ───────────────────

py::array typed_column_to_numpy(TypedColumn&& col) {
    return std::visit([](auto&& vec) -> py::array {
        using VecType = std::decay_t<decltype(vec)>;
        using T = typename VecType::value_type;

        auto heap_vec = std::make_unique<VecType>(std::move(vec));
        auto* raw = heap_vec.release();
        auto capsule = py::capsule(raw, [](void* p) {
            delete static_cast<VecType*>(p);
        });

        return py::array_t<T>(
            {static_cast<py::ssize_t>(raw->size())},
            {sizeof(T)},
            raw->data(),
            capsule
        );
    }, std::move(col));
}

py::dict single_result_to_python(SingleFileResult&& r) {
    py::list columns;
    py::list sensor_names;
    py::list sensor_units;
    py::list sensor_sizes;

    for (size_t i = 0; i < r.columns.size(); ++i) {
        if (r.start > 0 && r.n_records > 0) {
            py::array full_arr = typed_column_to_numpy(std::move(r.columns[i]));
            columns.append(full_arr[py::slice(py::int_(r.start), py::none(), py::none())]);
        } else {
            columns.append(typed_column_to_numpy(std::move(r.columns[i])));
        }
        sensor_names.append(r.sensor_info[i].name);
        sensor_units.append(r.sensor_info[i].units);
        sensor_sizes.append(r.sensor_info[i].size);
    }

    py::dict header;
    header["mission_name"] = r.header.mission_name;
    header["fileopen_time"] = r.header.fileopen_time;
    header["encoding_version"] = r.header.encoding_ver;
    header["full_filename"] = r.header.full_filename;
    header["sensor_list_crc"] = r.header.sensor_list_crc;
    header["the8x3_filename"] = r.header.the8x3_filename;
    header["filename_extension"] = r.header.filename_extension;

    py::dict out;
    out["columns"] = columns;
    out["sensor_names"] = sensor_names;
    out["sensor_units"] = sensor_units;
    out["sensor_sizes"] = sensor_sizes;
    out["n_records"] = r.n_records;
    out["header"] = header;
    out["filename"] = r.filename;
    return out;
}

py::dict multi_result_to_python(MultiFileResult&& r) {
    py::list columns;
    py::list sensor_names;
    py::list sensor_units;
    py::list sensor_sizes;

    for (size_t i = 0; i < r.columns.size(); ++i) {
        columns.append(typed_column_to_numpy(std::move(r.columns[i])));
        sensor_names.append(r.sensor_info[i].name);
        sensor_units.append(r.sensor_info[i].units);
        sensor_sizes.append(r.sensor_info[i].size);
    }

    py::dict out;
    out["columns"] = columns;
    out["sensor_names"] = sensor_names;
    out["sensor_units"] = sensor_units;
    out["sensor_sizes"] = sensor_sizes;
    out["n_records"] = r.n_records;
    out["n_files"] = r.n_files;
    return out;
}

} // anonymous namespace


PYBIND11_MODULE(_dbd_cpp, m, py::mod_gil_not_used()) {
    m.doc() = "C++ backend for reading Dinkum Binary Data (DBD) files";

    m.def("read_dbd_file",
        [](const std::string& filename,
           const std::string& cache_dir,
           const std::vector<std::string>& to_keep,
           const std::vector<std::string>& criteria,
           bool skip_first_record,
           bool repair) -> py::dict {
            // Parse entirely in C++ with GIL released
            SingleFileResult result;
            {
                py::gil_scoped_release release;
                result = parse_single_file(filename, cache_dir, to_keep,
                                           criteria, skip_first_record, repair);
            }
            // GIL reacquired — convert to Python objects
            return single_result_to_python(std::move(result));
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
        [](const std::vector<std::string>& filenames,
           const std::string& cache_dir,
           const std::vector<std::string>& to_keep,
           const std::vector<std::string>& criteria,
           const std::vector<std::string>& skip_missions,
           const std::vector<std::string>& keep_missions,
           bool skip_first_record,
           bool repair) -> py::dict {
            MultiFileResult result;
            {
                py::gil_scoped_release release;
                result = parse_multiple_files(filenames, cache_dir, to_keep,
                                              criteria, skip_missions,
                                              keep_missions, skip_first_record,
                                              repair);
            }
            return multi_result_to_python(std::move(result));
        },
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

    m.def("scan_sensors",
        [](const std::vector<std::string>& filenames,
           const std::string& cache_dir,
           const std::vector<std::string>& skip_missions,
           const std::vector<std::string>& keep_missions) -> py::dict {
            SensorListResult result;
            {
                py::gil_scoped_release release;
                result = scan_sensor_list(filenames, cache_dir,
                                          skip_missions, keep_missions);
            }
            py::list sensor_names;
            py::list sensor_units;
            py::list sensor_sizes;
            for (const auto& si : result.sensor_info) {
                sensor_names.append(si.name);
                sensor_units.append(si.units);
                sensor_sizes.append(si.size);
            }
            py::dict out;
            out["sensor_names"] = sensor_names;
            out["sensor_units"] = sensor_units;
            out["sensor_sizes"] = sensor_sizes;
            out["n_files"] = result.n_files;
            return out;
        },
        py::arg("filenames"),
        py::arg("cache_dir") = "",
        py::arg("skip_missions") = std::vector<std::string>(),
        py::arg("keep_missions") = std::vector<std::string>(),
        "Scan DBD file headers and return the unified sensor list without reading data.\n\n"
        "Returns a dict with keys: sensor_names, sensor_units, sensor_sizes, n_files"
    );

    m.def("scan_headers",
        [](const std::vector<std::string>& filenames,
           const std::vector<std::string>& skip_missions,
           const std::vector<std::string>& keep_missions) -> py::dict {
            HeaderScanResult result;
            {
                py::gil_scoped_release release;
                result = scan_file_headers(filenames, skip_missions, keep_missions);
            }
            py::list out_filenames;
            py::list out_missions;
            py::list out_crcs;
            for (const auto& fh : result.file_headers) {
                out_filenames.append(fh.filename);
                out_missions.append(fh.mission_name);
                out_crcs.append(fh.sensor_list_crc);
            }
            py::dict out;
            out["filenames"] = out_filenames;
            out["mission_names"] = out_missions;
            out["sensor_list_crcs"] = out_crcs;
            return out;
        },
        py::arg("filenames"),
        py::arg("skip_missions") = std::vector<std::string>(),
        py::arg("keep_missions") = std::vector<std::string>(),
        "Scan DBD file headers and return per-file mission names and CRCs.\n\n"
        "Returns a dict with keys: filenames, mission_names, sensor_list_crcs"
    );
}
