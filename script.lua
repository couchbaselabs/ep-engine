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
