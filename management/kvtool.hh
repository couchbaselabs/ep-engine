#include <memcached/extension.h>
#include <memcached/engine.h>

#ifdef __cplusplus
extern "C" {
#endif
bool load_engine(const char *soname,
                 SERVER_HANDLE_V1 *(*get_server_api)(void),
                 EXTENSION_LOGGER_DESCRIPTOR *logger,
                 ENGINE_HANDLE **engine_handle);

bool init_engine(ENGINE_HANDLE * engine,
                 const char *config_str,
                 EXTENSION_LOGGER_DESCRIPTOR *logger);

void log_engine_details(ENGINE_HANDLE * engine,
                        EXTENSION_LOGGER_DESCRIPTOR *logger);

#ifdef __cplusplus
}
#endif
