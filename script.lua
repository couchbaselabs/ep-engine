ascii_extensions = {}

-- function setup_extensions(register)
--    io.write("Initializing extensions from the lua script.\n")
--    handler = {}
--    handler["accept"] = function (cookie, argc, argv, ndata, ptr)
--                           io.write("Asking if we accept " .. argv[0] .. "\n")
--                           return true
--                        end
--    handler["abort"] = function (cookie)
--                       end
--    handler["execute"] = function (cookie, argc, argv, handler)
--                         end
--    register("test", handler)
-- end

-- io.write("Initialized scripting support.\n")

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
