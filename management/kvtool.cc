#include <iostream>
#include <cassert>
#include <sysexits.h>
#include <memcached/engine_testapp.h>
#include <memcached/extension_loggers.h>

#include <lua.hpp>

#include "management/script_server.h"
#include "management/kvtool.hh"

EXTENSION_LOGGER_DESCRIPTOR *logger_descriptor = NULL;

extern "C" {

    static int engine_load(lua_State *ls) {
        if (lua_gettop(ls) != 1) {
            lua_pushstring(ls, "engine.load takes one argument: "
                           "\"path_to_engine\"");
            lua_error(ls);
            return 1;
        }

        const char *engine_name = luaL_checkstring(ls, 1);

        ENGINE_HANDLE **h = static_cast<ENGINE_HANDLE**>(lua_newuserdata(ls, sizeof(void*)));

        if (!load_engine(engine_name, &get_script_server_api, logger_descriptor, h)) {
            char msg[1024];
            snprintf(msg, sizeof(msg) - 1, "Failed to load engine %s.\n", engine_name);
            lua_pushstring(ls, msg);
            lua_error(ls);
            return 1;
        }

        luaL_getmetatable(ls, "engine");
        lua_setmetatable(ls, -2);

        return 1;
    }

    static ENGINE_HANDLE *getHandle(lua_State *ls) {
        ENGINE_HANDLE **hh = static_cast<ENGINE_HANDLE**>(luaL_checkudata(ls, 1, "engine"));
        assert(hh);
        assert(*hh);
        return *hh;
    }

    static ENGINE_HANDLE_V1 *getHandle1(ENGINE_HANDLE *h) {
        return reinterpret_cast<ENGINE_HANDLE_V1*>(h);
    }

    static int engine_init(lua_State *ls) {
        if (lua_gettop(ls) != 2) {
            lua_pushstring(ls, "engine:init takes two arguments: "
                           "engine_handle, \"config_params\"");
            lua_error(ls);
            return 1;
        }

        const char *cfg = luaL_checkstring(ls, 2);

        ENGINE_HANDLE *h = getHandle(ls);
        if(!init_engine(h, cfg, logger_descriptor)) {
            lua_pushstring(ls, "Failed to init engine");
            lua_error(ls);
            return 1;
        }

        return 0;
    }

    static int engine_destroy(lua_State *ls) {
        if (lua_gettop(ls) != 1) {
            lua_pushstring(ls, "engine:destroy takes one arguments: "
                           "engine_handle");
            lua_error(ls);
            return 1;
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);
        h1->destroy(h, false);

        return 0;
    }

    static int engine_allocate(lua_State *ls) {
        if (lua_gettop(ls) < 3) {
            lua_pushstring(ls, "engine:allocate takes 3 arguments: "
                           "engine_handle, key, size");
            lua_error(ls);
            return 1;
        }

        item *item;

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);

        const char *key = luaL_checkstring(ls, 2);
        size_t alloc_size = static_cast<size_t>(luaL_checkint(ls, 3));

        ENGINE_ERROR_CODE rc = h1->allocate(h, NULL, &item, key, strlen(key), alloc_size, 0, 0);
        if (rc == ENGINE_SUCCESS) {
            lua_pushlightuserdata(ls, item);
        } else {
            lua_pushstring(ls, "Allocation failure");
            lua_error(ls);
        }

        return 1;
    }

    static int engine_release(lua_State *ls) {
        if (lua_gettop(ls) < 2) {
            lua_pushstring(ls, "engine:release takes 2 arguments: "
                           "engine_handle, item");
            lua_error(ls);
            return 1;
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);
        item *item = static_cast<ENGINE_HANDLE_V1*>(lua_touserdata(ls, 2));

        h1->release(h, NULL, item);

        return 0;
    }

    static int engine_flush(lua_State *ls) {
        if (lua_gettop(ls) < 1) {
            lua_pushstring(ls, "engine:flush takes 1 argument: "
                           "engine_handle");
            lua_error(ls);
            return 1;
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);

        if (h1->flush(h, NULL, 0) != ENGINE_SUCCESS) {
            lua_pushstring(ls, "Error flushing");
            lua_error(ls);
            return 1;
        }

        return 0;
    }

    static void add_stats_fun(const char *key, const uint16_t klen,
                              const char *val, const uint32_t vlen,
                              const void *cookie) {
        // This is kind of abusy, but I'll take it
        lua_State *ls(const_cast<lua_State*>(static_cast<const lua_State*>(cookie)));

        assert(lua_istable(ls, 1));
        lua_pushlstring(ls, key, klen);
        lua_pushlstring(ls, val, vlen);
        assert(lua_istable(ls, -3));
        lua_settable(ls, -3);
    }

    static int engine_stats(lua_State *ls) {
        const char *which=NULL;
        if (lua_gettop(ls) < 1) {
            lua_pushstring(ls, "engine:stats takes at least 1 argument: "
                           "engine_handle, [\"which_stats\"]");
            lua_error(ls);
            return 1;
        }
        if (lua_gettop(ls) > 2) {
            which = luaL_checkstring(ls, 2);
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);

        lua_settop(ls, 0);
        lua_newtable(ls);

        ENGINE_ERROR_CODE rc = h1->get_stats(h, ls, which, which == NULL ? 0 : strlen(which), add_stats_fun);
        if (rc != ENGINE_SUCCESS) {
            lua_pushstring(ls, "Error statting");
            lua_error(ls);
        }

        assert(lua_istable(ls, 1));

        return 1;
    }

    static int engine_get(lua_State *ls) {
        if (lua_gettop(ls) < 3) {
            lua_pushstring(ls, "What item do want to get (vbid, key)?");
            lua_error(ls);
            return 1;
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);
        uint16_t vbid = luaL_checkint(ls, 2);
        const char *key = luaL_checkstring(ls, 3);

        item *i;

        if (h1->get(h, NULL, &i, key, strlen(key), vbid) != ENGINE_SUCCESS) {
            lua_pushstring(ls, "Could not get item");
            lua_error(ls);
            return 1;
        }

        item_info info;
        if (!h1->get_item_info(h, NULL, i, &info)) {
            lua_pushstring(ls, "Couldn't get the info.");
            lua_error(ls);

            return 1;
        }

        assert(info.nvalue == 1);

        lua_settop(ls, 0);

        lua_pushlstring(ls, static_cast<char*>(info.value[0].iov_base),
                        info.value[0].iov_len);
        lua_pushinteger(ls, info.flags);
        lua_pushinteger(ls, info.cas);
        lua_pushinteger(ls, info.exptime);

        return 4;
    }

    static int engine_rm(lua_State *ls) {
        if (lua_gettop(ls) < 3) {
            lua_pushstring(ls, "What item do want to get (vbid, key)?");
            lua_error(ls);
            return 1;
        }

        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);
        uint16_t vbid = luaL_checkint(ls, 2);
        const char *key = luaL_checkstring(ls, 3);

        if (h1->remove(h, NULL, key, strlen(key), 0, vbid) != ENGINE_SUCCESS) {
            lua_pushstring(ls, "Could not remove item");
            lua_error(ls);
            return 1;
        }
        return 0;
    }

    static int engine_store(lua_State *ls, ENGINE_STORE_OPERATION op, uint64_t cas) {
        if (lua_gettop(ls) < 6) {
            lua_pushstring(ls, "usage: f(handle, vb, key, flags, exp, val)");
            lua_error(ls);
            return 1;
        }

        size_t vallen;
        ENGINE_HANDLE *h = getHandle(ls);
        ENGINE_HANDLE_V1 *h1 = getHandle1(h);
        uint16_t vbid = luaL_checkint(ls, 2);
        const char *key = luaL_checkstring(ls, 3);
        int flags = luaL_checkint(ls, 4);
        int exp = luaL_checkint(ls, 5);
        const char *val = luaL_checklstring(ls, 6, &vallen);

        item *it;
        ENGINE_ERROR_CODE rc = h1->allocate(h, NULL, &it,
                                            key, strlen(key), vallen, flags, exp);
        if (rc != ENGINE_SUCCESS) {
            lua_pushstring(ls, "Allocation failure");
            lua_error(ls);
        }

        item_info info;
        if (!h1->get_item_info(h, NULL, it, &info)) {
            lua_pushstring(ls, "Couldn't get the info.");
            lua_error(ls);
            return 1;
        }

        assert(info.nvalue == 1);
        if (op == OPERATION_CAS) {
            h1->item_set_cas(h, NULL, it, cas);
        }
        info.flags = flags;
        info.exptime = exp;
        memcpy(info.value[0].iov_base, val, vallen);
        info.value[0].iov_len = vallen;

        rc =  h1->store(h, NULL, it, &cas, op, vbid);
        if (rc != ENGINE_SUCCESS) {
            char msg[1024];
            snprintf(msg, sizeof(msg), "Error storing value with op %d: %d", op, rc);
            lua_pushstring(ls, msg);
            lua_error(ls);
        } else {
            lua_pushinteger(ls, cas);
        }
        return 1;
    }

    static int engine_set(lua_State *ls) {
        return engine_store(ls, OPERATION_SET, 0);
    }

    static int engine_add(lua_State *ls) {
        return engine_store(ls, OPERATION_ADD, 0);
    }

    static int engine_append(lua_State *ls) {
        return engine_store(ls, OPERATION_APPEND, 0);
    }

    static int engine_prepend(lua_State *ls) {
        return engine_store(ls, OPERATION_PREPEND, 0);
    }

    static int engine_cas(lua_State *ls) {
        if (lua_gettop(ls) < 7) {
            lua_pushstring(ls, "usage: cas(handle, vb, key, flags, exp, val, casval)");
            lua_error(ls);
            return 1;
        }
        return engine_store(ls, OPERATION_CAS, luaL_checkint(ls, 7));
    }

    static const luaL_Reg engine_funcs[] = {
        {"load", engine_load},
        {NULL, NULL}
    };

    static const luaL_Reg engine_methods[] = {
        {"init", engine_init},
        {"destroy", engine_destroy},
        {"allocate", engine_allocate},
        {"release", engine_release},
        {"flush", engine_flush},
        {"stats", engine_stats},
        {"get", engine_get},
        {"set", engine_set},
        {"add", engine_add},
        {"append", engine_append},
        {"prepend", engine_prepend},
        {"cas", engine_cas},
        {"rm", engine_rm},
        {NULL, NULL}
    };

}

int main(int argc, char **argv) {

    if (argc < 2) {
        std::cerr << "Give me a filename or give me death." << std::endl;
        exit(EX_USAGE);
    }

    logger_descriptor = get_stderr_logger();

    init_script_server();

    lua_State *ls = luaL_newstate();
    luaL_openlibs(ls);

    luaL_newmetatable(ls, "engine");

    lua_pushstring(ls, "__index");
    lua_pushvalue(ls, -2);  /* pushes the metatable */
    lua_settable(ls, -3);  /* metatable.__index = metatable */

    luaL_openlib(ls, NULL, engine_methods, 0);

    luaL_openlib(ls, "engine", engine_funcs, 0);

    int rv(luaL_dofile(ls, argv[1]));
    if (rv != 0) {
        std::cerr << "Error running stuff:  " << lua_tostring(ls, -1) << std::endl;
    }
    return rv;
}
