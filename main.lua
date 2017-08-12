--[[
This script reads a JSON feed from pmacct's pmbgpd and maintains a table in postgres with route data
]]

local cjson = require "cjson"
local pg = require "pg" -- https://github.com/daurnimator/lua-pg

local conn = assert(pg.connectdb())

local update_statement = assert(conn:prepare([[
insert into routes (peer_ip_src, subnet, as_path, bgp_nexthop, local_pref)
	values ($1, $2, $3, $4, $5)
	on conflict (peer_ip_src, subnet) do update set
		as_path=excluded.as_path,
		bgp_nexthop=excluded.bgp_nexthop,
		local_pref=excluded.local_pref
]], pg.oids.unknown, pg.oids.unknown, pg.oids.unknown, pg.oids.unknown, pg.oids.float8))

local delete_statement = assert(conn:prepare([[
delete from routes where
	peer_ip_src = $1
]], pg.oids.unknown))

for line in io.stdin:lines() do
	local decoded = cjson.decode(line)
	if decoded.event_type == "log" then
		local as_path = "{" .. decoded.as_path:gsub(" ", ",") .. "}" -- transform into postgres array literal
		local local_pref = math.tointeger(decoded.local_pref)
		assert(update_statement:exec(
			decoded.peer_ip_src,
			decoded.ip_prefix,
			as_path,
			decoded.bgp_nexthop,
			local_pref))
		io.stderr:write(string.format("Route %s from %s updated\n", decoded.ip_prefix, decoded.peer_ip_src))
	elseif decoded.event_type == "log_close" then
		assert(delete_statement:exec(decoded.peer_ip_src))
		io.stderr:write(string.format("Removed routes from %s\n", decoded.peer_ip_src))
	elseif decoded.event_type == "log_init" then
		-- do nothing
		-- TODO: clear out old routes from a crash?
	else
		io.stderr:write(string.format("Unknown event type: %s\n", decoded.event_type))
	end
end

conn:close()
