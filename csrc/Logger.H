// Logger.H - No-op stub replacing spdlog for the Python extension module
// The original dbd2netCDF uses spdlog; here we discard all log output.

#ifndef INC_Logger_H
#define INC_Logger_H

#define LOG_TRACE(...)
#define LOG_DEBUG(...)
#define LOG_INFO(...)
#define LOG_WARN(...)
#define LOG_ERROR(...)
#define LOG_CRITICAL(...)

#endif // INC_Logger_H
