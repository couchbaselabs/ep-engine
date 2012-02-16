#ifndef MEMCACHED_SCRIPT_SERVER_H
#define MEMCACHED_SCRIPT_SERVER_H

#include <memcached/engine.h>

#ifdef  __cplusplus
extern "C" {
#endif

struct script_connstruct {
    uint64_t magic;
    const char *uname;
    const char *config;
    void *engine_data;
    bool connected;
    struct script_connstruct *next;
    int sfd;
    ENGINE_ERROR_CODE status;
    uint64_t evictions;
    int nblocks; /* number of ewouldblocks */
    bool handle_ewouldblock;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
};

struct script_callbacks {
    EVENT_CALLBACK cb;
    const void *cb_data;
    struct script_callbacks *next;
};

struct script_stats {
    uint64_t astat;
};

MEMCACHED_PUBLIC_API SERVER_HANDLE_V1 *get_script_server_api(void);

MEMCACHED_PUBLIC_API void init_script_server(void);

MEMCACHED_PUBLIC_API
struct script_connstruct *mk_script_connection(const char *user,
                                           const char *config);

MEMCACHED_PUBLIC_API const void *create_script_cookie(void);

MEMCACHED_PUBLIC_API void destroy_script_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void script_set_ewouldblock_handling(const void *cookie, bool enable);

MEMCACHED_PUBLIC_API void lock_script_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void unlock_script_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void waitfor_script_cookie(const void *cookie);

MEMCACHED_PUBLIC_API void script_time_travel(int by);

MEMCACHED_PUBLIC_API void disconnect_script_connection(struct script_connstruct *c);

MEMCACHED_PUBLIC_API void disconnect_all_script_connections(struct script_connstruct *c);

MEMCACHED_PUBLIC_API void destroy_script_event_callbacks_rec(struct script_callbacks *h);

MEMCACHED_PUBLIC_API void destroy_script_event_callbacks(void);

#ifdef  __cplusplus
}
#endif

#endif  /* MEMCACHED_SCRIPT_SERVER_H */
