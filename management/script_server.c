#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <pthread.h>
#include <memcached/engine.h>
#include <memcached/extension.h>
#include <memcached/extension_loggers.h>

#include <management/script_server.h>

#define REALTIME_MAXDELTA 60*60*24*3
#define CONN_MAGIC 16369814453946373207ULL

struct script_extensions {
    EXTENSION_DAEMON_DESCRIPTOR *daemons;
    EXTENSION_LOGGER_DESCRIPTOR *logger;
};

struct script_callbacks *script_event_handlers[MAX_ENGINE_EVENT_TYPE + 1];
time_t process_started;     /* when the script server was started */
rel_time_t time_travel_offset;
rel_time_t current_time;
struct script_connstruct *connstructs;
struct script_extensions extensions;
EXTENSION_LOGGER_DESCRIPTOR *null_logger = NULL;
EXTENSION_LOGGER_DESCRIPTOR *stderr_logger = NULL;

/**
 * SERVER CORE API FUNCTIONS
 */

static void script_get_auth_data(const void *cookie, auth_data_t *data) {
    struct script_connstruct *c = (struct script_connstruct *)cookie;
    if (c != NULL) {
        data->username = c->uname;
        data->config = c->config;
    }
}

static void script_store_engine_specific(const void *cookie, void *engine_data) {
    if (cookie) {
        struct script_connstruct *c = (struct script_connstruct *)cookie;
        assert(c->magic == CONN_MAGIC);
        c->engine_data = engine_data;
    }
}

static void *script_get_engine_specific(const void *cookie) {
    struct script_connstruct *c = (struct script_connstruct *)cookie;
    assert(c == NULL || c->magic == CONN_MAGIC);
    return c ? c->engine_data : NULL;
}

static int script_get_socket_fd(const void *cookie) {
    struct script_connstruct *c = (struct script_connstruct *)cookie;
    return c->sfd;
}

static ENGINE_ERROR_CODE script_cookie_reserve(const void *cookie) {
    (void)cookie;
    return ENGINE_SUCCESS;
}

static ENGINE_ERROR_CODE script_cookie_release(const void *cookie) {
    (void)cookie;
    return ENGINE_SUCCESS;
}

static const char *script_get_server_version(void) {
    return "script server";
}

#define HASH_LITTLE_ENDIAN 1

static uint32_t script_hash( const void *key, size_t length, const uint32_t initval) {
    uint32_t a,b,c;
    union { const void *ptr; size_t i; } u;

    /* Set up the internal state */
    a = b = c = 0xdeadbeef + ((uint32_t)length) + initval;

#define rot(x,k) (((x)<<(k)) ^ ((x)>>(32-(k))))

#define mix(a,b,c) {                \
        a -= c;  a ^= rot(c, 4);  c += b;       \
        b -= a;  b ^= rot(a, 6);  a += c;       \
        c -= b;  c ^= rot(b, 8);  b += a;       \
        a -= c;  a ^= rot(c,16);  c += b;       \
        b -= a;  b ^= rot(a,19);  a += c;       \
        c -= b;  c ^= rot(b, 4);  b += a;       \
    }

    u.ptr = key;
    if (HASH_LITTLE_ENDIAN && ((u.i & 0x3) == 0)) {
        const uint32_t *k = key;                           /* read 32-bit chunks */

        /*------ all but last block: aligned reads and affect 32 bits of (a,b,c) */
        while (length > 12) {
            a += k[0];
            b += k[1];
            c += k[2];
            mix(a,b,c);
            length -= 12;
            k += 3;
        }

        /*----------------------------- handle the last (probably partial) block */
        /*
         * "k[2]&0xffffff" actually reads beyond the end of the string, but
         * then masks off the part it's not allowed to read.  Because the
         * string is aligned, the masked-off tail is in the same word as the
         * rest of the string.  Every machine with memory protection I've seen
         * does it on word boundaries, so is OK with this.  But VALGRIND will
         * still catch it and complain.  The masking trick does make the hash
         * noticably faster for short strings (like English words).
         */
        switch(length) {
        case 12: c+=k[2]; b+=k[1]; a+=k[0]; break;
        case 11: c+=k[2]&0xffffff; b+=k[1]; a+=k[0]; break;
        case 10: c+=k[2]&0xffff; b+=k[1]; a+=k[0]; break;
        case 9 : c+=k[2]&0xff; b+=k[1]; a+=k[0]; break;
        case 8 : b+=k[1]; a+=k[0]; break;
        case 7 : b+=k[1]&0xffffff; a+=k[0]; break;
        case 6 : b+=k[1]&0xffff; a+=k[0]; break;
        case 5 : b+=k[1]&0xff; a+=k[0]; break;
        case 4 : a+=k[0]; break;
        case 3 : a+=k[0]&0xffffff; break;
        case 2 : a+=k[0]&0xffff; break;
        case 1 : a+=k[0]&0xff; break;
        case 0 : return c;  /* zero length strings require no mixing */
        }

    } else if (HASH_LITTLE_ENDIAN && ((u.i & 0x1) == 0)) {
        const uint16_t *k = key;                           /* read 16-bit chunks */
        const uint8_t  *k8;

        /*--------------- all but last block: aligned reads and different mixing */
        while (length > 12) {
            a += k[0] + (((uint32_t)k[1])<<16);
            b += k[2] + (((uint32_t)k[3])<<16);
            c += k[4] + (((uint32_t)k[5])<<16);
            mix(a,b,c);
            length -= 12;
            k += 6;
        }

        /*----------------------------- handle the last (probably partial) block */
        k8 = (const uint8_t *)k;
        switch(length) {
        case 12: c+=k[4]+(((uint32_t)k[5])<<16);
            b+=k[2]+(((uint32_t)k[3])<<16);
            a+=k[0]+(((uint32_t)k[1])<<16);
            break;
        case 11: c+=((uint32_t)k8[10])<<16;     /* @fallthrough */
        case 10: c+=k[4];                       /* @fallthrough@ */
            b+=k[2]+(((uint32_t)k[3])<<16);
            a+=k[0]+(((uint32_t)k[1])<<16);
            break;
        case 9 : c+=k8[8];                      /* @fallthrough */
        case 8 : b+=k[2]+(((uint32_t)k[3])<<16);
            a+=k[0]+(((uint32_t)k[1])<<16);
            break;
        case 7 : b+=((uint32_t)k8[6])<<16;      /* @fallthrough */
        case 6 : b+=k[2];
            a+=k[0]+(((uint32_t)k[1])<<16);
            break;
        case 5 : b+=k8[4];                      /* @fallthrough */
        case 4 : a+=k[0]+(((uint32_t)k[1])<<16);
            break;
        case 3 : a+=((uint32_t)k8[2])<<16;      /* @fallthrough */
        case 2 : a+=k[0];
            break;
        case 1 : a+=k8[0];
            break;
        case 0 : return c;  /* zero length strings require no mixing */
        }
    } else {                        /* need to read the key one byte at a time */
        const uint8_t *k = key;

        /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
        while (length > 12) {
            a += k[0];
            a += ((uint32_t)k[1])<<8;
            a += ((uint32_t)k[2])<<16;
            a += ((uint32_t)k[3])<<24;
            b += k[4];
            b += ((uint32_t)k[5])<<8;
            b += ((uint32_t)k[6])<<16;
            b += ((uint32_t)k[7])<<24;
            c += k[8];
            c += ((uint32_t)k[9])<<8;
            c += ((uint32_t)k[10])<<16;
            c += ((uint32_t)k[11])<<24;
            mix(a,b,c);
            length -= 12;
            k += 12;
        }

        /*-------------------------------- last block: affect all 32 bits of (c) */
        switch(length) {
        case 12: c+=((uint32_t)k[11])<<24;
        case 11: c+=((uint32_t)k[10])<<16;
        case 10: c+=((uint32_t)k[9])<<8;
        case 9 : c+=k[8];
        case 8 : b+=((uint32_t)k[7])<<24;
        case 7 : b+=((uint32_t)k[6])<<16;
        case 6 : b+=((uint32_t)k[5])<<8;
        case 5 : b+=k[4];
        case 4 : a+=((uint32_t)k[3])<<24;
        case 3 : a+=((uint32_t)k[2])<<16;
        case 2 : a+=((uint32_t)k[1])<<8;
        case 1 : a+=k[0];
            break;
        case 0 : return c;  /* zero length strings require no mixing */
        }
    }

#define final(a,b,c)                            \
    {                                           \
        c ^= b; c -= rot(b,14);                 \
        a ^= c; a -= rot(c,11);                 \
        b ^= a; b -= rot(a,25);                 \
        c ^= b; c -= rot(b,16);                 \
        a ^= c; a -= rot(c,4);                  \
        b ^= a; b -= rot(a,14);                 \
        c ^= b; c -= rot(b,24);                 \
    }

    final(a,b,c);
    return c;             /* zero length strings require no mixing */
}

/* time-sensitive callers can call it by hand with this, outside the
   normal ever-1-second timer */
static rel_time_t script_get_current_time(void) {
    struct timeval timer;
    gettimeofday(&timer, NULL);
    current_time = (rel_time_t) (timer.tv_sec - process_started + time_travel_offset);
    return current_time;
}

static rel_time_t script_realtime(const time_t exptime) {
    /* no. of seconds in 30 days - largest possible delta exptime */

    if (exptime == 0) return 0; /* 0 means never expire */

    if (exptime > REALTIME_MAXDELTA) {
        /* if item expiration is at/before the server started, give it an
           expiration time of 1 second after the server started.
           (because 0 means don't expire).  without this, we'd
           underflow and wrap around to some large value way in the
           future, effectively making items expiring in the past
           really expiring never */
        if (exptime <= process_started)
            return (rel_time_t)1;
        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + script_get_current_time());
    }
}

static void script_notify_io_complete(const void *cookie, ENGINE_ERROR_CODE status) {
    struct script_connstruct *c = (struct script_connstruct *)cookie;
    pthread_mutex_lock(&c->mutex);
    c->status = status;
    pthread_cond_signal(&c->cond);
    pthread_mutex_unlock(&c->mutex);
}

static time_t script_abstime(const rel_time_t exptime)
{
    return process_started + exptime;
}

void script_time_travel(int by) {
    time_travel_offset += by;
}

static int script_parse_config(const char *str, struct config_item items[], FILE *error) {
    return parse_config(str, items, error);
}

/**
 * SERVER STAT API FUNCTIONS
 */

static void *script_new_independent_stats(void) {
    struct scriptstats *scriptstats = calloc(sizeof(scriptstats),1);
    return scriptstats;
}

static void script_release_independent_stats(void *stats) {
    struct scriptstats *scriptstats = stats;
    free(scriptstats);
}

static void script_count_eviction(const void *cookie, const void *key, const int nkey) {
    (void)key;
    (void)nkey;
    struct script_connstruct *c = (struct script_connstruct *)cookie;
    c->evictions++;
}

/**
 * SERVER STAT API FUNCTIONS
 */

static bool script_register_extension(extension_type_t type, void *extension)
{
    if (extension == NULL) {
        return false;
    }

    switch (type) {
    case EXTENSION_DAEMON:
        for (EXTENSION_DAEMON_DESCRIPTOR *ptr =  extensions.daemons;
             ptr != NULL;
             ptr = ptr->next) {
            if (ptr == extension) {
                return false;
            }
        }
        ((EXTENSION_DAEMON_DESCRIPTOR *)(extension))->next = extensions.daemons;
        extensions.daemons = extension;
        return true;
    case EXTENSION_LOGGER:
        extensions.logger = extension;
        return true;
    default:
        return false;
    }
}

static void script_unregister_extension(extension_type_t type, void *extension)
{
    switch (type) {
    case EXTENSION_DAEMON:
        {
            EXTENSION_DAEMON_DESCRIPTOR *prev = NULL;
            EXTENSION_DAEMON_DESCRIPTOR *ptr = extensions.daemons;

            while (ptr != NULL && ptr != extension) {
                prev = ptr;
                ptr = ptr->next;
            }

            if (ptr != NULL && prev != NULL) {
                prev->next = ptr->next;
            }

            if (extensions.daemons == ptr) {
                extensions.daemons = ptr->next;
            }
        }
        break;
    case EXTENSION_LOGGER:
        if (extensions.logger == extension) {
            if (stderr_logger == extension) {
                extensions.logger = null_logger;
            } else {
                extensions.logger = stderr_logger;
            }
        }
        break;

    default:
        ;
    }

}

static void* script_get_extension(extension_type_t type)
{
    switch (type) {
    case EXTENSION_DAEMON:
        return extensions.daemons;

    case EXTENSION_LOGGER:
        return extensions.logger;

    default:
        return NULL;
    }
}

/**
 * SERVER CALLBACK API FUNCTIONS
 */

static void script_register_callback(ENGINE_HANDLE *eh,
                                   ENGINE_EVENT_TYPE type,
                                   EVENT_CALLBACK cb,
                                   const void *cb_data) {
    (void)eh;
    struct script_callbacks *h =
        calloc(sizeof(struct script_callbacks), 1);
    assert(h);
    h->cb = cb;
    h->cb_data = cb_data;
    h->next = script_event_handlers[type];
    script_event_handlers[type] = h;
}

static void script_perform_callbacks(ENGINE_EVENT_TYPE type,
                                   const void *data,
                                   const void *c) {
    for (struct script_callbacks *h = script_event_handlers[type];
         h; h = h->next) {
        h->cb(c, type, data, h->cb_data);
    }
}

SERVER_HANDLE_V1 *get_script_server_api(void)
{
    static SERVER_CORE_API core_api = {
        .server_version = script_get_server_version,
        .hash = script_hash,
        .realtime = script_realtime,
        .get_current_time = script_get_current_time,
        .abstime = script_abstime,
        .parse_config = script_parse_config
    };

    static SERVER_COOKIE_API server_cookie_api = {
        .get_auth_data = script_get_auth_data,
        .store_engine_specific = script_store_engine_specific,
        .get_engine_specific = script_get_engine_specific,
        .get_socket_fd = script_get_socket_fd,
        .notify_io_complete = script_notify_io_complete,
        .reserve = script_cookie_reserve,
        .release = script_cookie_release
    };

    static SERVER_STAT_API server_stat_api = {
        .new_stats = script_new_independent_stats,
        .release_stats = script_release_independent_stats,
        .evicting = script_count_eviction
    };

    static SERVER_EXTENSION_API extension_api = {
        .register_extension = script_register_extension,
        .unregister_extension = script_unregister_extension,
        .get_extension = script_get_extension
    };

    static SERVER_CALLBACK_API callback_api = {
        .register_callback = script_register_callback,
        .perform_callbacks = script_perform_callbacks
    };

    static SERVER_HANDLE_V1 rv = {
        .interface = 1,
        .core = &core_api,
        .stat = &server_stat_api,
        .extension = &extension_api,
        .callback = &callback_api,
        .cookie = &server_cookie_api
    };

    return &rv;
}

void init_script_server() {
    process_started = time(0);
    null_logger = get_null_logger();
    stderr_logger = get_stderr_logger();
    extensions.logger = null_logger;
}

struct script_connstruct *mk_script_connection(const char *user, const char *config) {
    struct script_connstruct *rv = calloc(sizeof(struct script_connstruct), 1);
    auth_data_t ad;
    assert(rv);
    rv->magic = CONN_MAGIC;
    rv->uname = user ? strdup(user) : NULL;
    rv->config = config ? strdup(config) : NULL;
    rv->connected = true;
    rv->next = connstructs;
    rv->evictions = 0;
    rv->sfd = 0; //TODO make this more realistic
    rv->status = ENGINE_SUCCESS;
    connstructs = rv;
    script_perform_callbacks(ON_CONNECT, NULL, rv);
    if (rv->uname) {
        script_get_auth_data(rv, &ad);
        script_perform_callbacks(ON_AUTH, (const void*)&ad, rv);
    }

    assert(pthread_mutex_init(&rv->mutex, NULL) == 0);
    assert(pthread_cond_init(&rv->cond, NULL) == 0);

    return rv;
}

const void *create_script_cookie(void) {
    struct script_connstruct *rv = calloc(sizeof(struct script_connstruct), 1);
    assert(rv);
    rv->magic = CONN_MAGIC;
    rv->connected = true;
    rv->status = ENGINE_SUCCESS;
    rv->handle_ewouldblock = true;
    assert(pthread_mutex_init(&rv->mutex, NULL) == 0);
    assert(pthread_cond_init(&rv->cond, NULL) == 0);

    return rv;
}

void destroy_script_cookie(const void *cookie) {
    free((void*)cookie);
}

void script_set_ewouldblock_handling(const void *cookie, bool enable) {
    struct script_connstruct *v = (void*)cookie;
    v->handle_ewouldblock = enable;
}

void lock_script_cookie(const void *cookie) {
   struct script_connstruct *c = (void*)cookie;
   pthread_mutex_lock(&c->mutex);
}

void unlock_script_cookie(const void *cookie) {
   struct script_connstruct *c = (void*)cookie;
   pthread_mutex_unlock(&c->mutex);
}

void waitfor_script_cookie(const void *cookie) {
   struct script_connstruct *c = (void*)cookie;
   pthread_cond_wait(&c->cond, &c->mutex);
}

void disconnect_script_connection(struct script_connstruct *c) {
    c->connected = false;
    script_perform_callbacks(ON_DISCONNECT, NULL, c);
}

void disconnect_all_script_connections(struct script_connstruct *c) {
    if (c) {
        disconnect_script_connection(c);
        disconnect_all_script_connections(c->next);
        free((void*)c->uname);
        free((void*)c->config);
        free(c);
    }
}

void destroy_script_event_callbacks_rec(struct script_callbacks *h) {
    if (h) {
        destroy_script_event_callbacks_rec(h->next);
        free(h);
    }
}

void destroy_script_event_callbacks(void) {
    int i = 0;
    for (i = 0; i < MAX_ENGINE_EVENT_TYPE; i++) {
        destroy_script_event_callbacks_rec(script_event_handlers[i]);
        script_event_handlers[i] = NULL;
    }
}
