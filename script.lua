mc_ext = {}

mc_ext.cas = function(vb, k, f)
   local newvalue
   while true do
      local flags, cas, value = mc.get(vb, k)
      newvalue = f(value)
      if pcall(mc.set, vb, k, 0, flags, newvalue, cas) then
         break
      end
   end
   return newvalue
end

function mc_post_init()
   local __global = _G

   -- This is kind of a debuggish function that lets us see all of the
   -- globals that are defined since we can't get to them with _G
   mc_ext.dumpGlobals = function()
                           local rv = {}
                           for k in pairs(_G) do
                              table.insert(rv, k)
                           end
                           table.sort(rv)
                           return rv
                        end

   local global_metatable = {
      __index = function(t, k) return __global[k] end,
      __newindex = function(t, k, v)
                      if type(v) ~= 'function' then
                         error("Only global functions are tracked.")
                      end
                      ep_core.register_global(k, v)
                      __global[k] = v
                   end
   }

   _G = {}
   setmetatable(_G, global_metatable)
   setfenv(0, _G)

end
