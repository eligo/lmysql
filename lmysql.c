#include "lmysql.h"
#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <stdio.h>
#include <mysql.h>
#include <string.h>
#include <stdlib.h>

#define CNAME_CONNECTION "LUA_MYSQL_CONNECTION"
#define CNAME_CURSOR "LUA_MYSQL_CURSOR"
struct sql_t {
	size_t cap;
	size_t cur;
	char* ptr;
};

struct mysql_t {
	MYSQL * conn;
	int closed;
	struct sql_t sql;
	struct sql_t fs;
	struct sql_t vs;
};

struct cursor_t {
	MYSQL_RES* res;
	int closed;
	size_t cols;
	size_t rows;
};

void sql_init(struct sql_t* sql) {
	memset(sql, 0, sizeof(*sql));
}

void sql_cat(struct sql_t* sql, const char* ptr, size_t len, MYSQL* conn) {
	size_t need = len;
	size_t fsz = sql->cap - sql->cur;
	if (conn)
		need *= 2 + 1;
	if (fsz < need) {
		size_t ncap = sql->cap+need+1;
		if (ncap < sql->cap) return;
		char* nptr = realloc(sql->ptr, ncap);
		if (!nptr) return;
		sql->ptr = nptr;
		sql->cap = ncap;
	}
	if (conn) {
		size_t nlen = mysql_real_escape_string(conn, sql->ptr+sql->cur, ptr, len);
		sql->cur += nlen;
	} else {
		memcpy(sql->ptr+sql->cur, ptr, len);
		sql->cur+=len;
	}
	sql->ptr[sql->cur]='\0';
}

void sql_reset(struct sql_t* sql) {
	sql->cur = 0;
}

int l_faildirect(lua_State *L, const char *err) {
    lua_pushnil(L);
    lua_pushstring(L, err);
    return 2;
}

/*cocur*/
int cursor_close(struct lua_State* L) {
	struct cursor_t* cur = (struct cursor_t*)luaL_checkudata (L, 1, CNAME_CURSOR);
	if (!cur->closed) {
		mysql_free_result(cur->res);
		cur->closed = 1;
	}
	return 0;
}

int cursor_fetch_row(struct lua_State* L) {
	struct cursor_t* cur = (struct cursor_t*)luaL_checkudata (L, 1, CNAME_CURSOR);
	MYSQL_FIELD* fields;
	size_t* lens;
	int i;
	fields = mysql_fetch_fields(cur->res);
	MYSQL_ROW row = mysql_fetch_row(cur->res);
	if (!row) {
		lua_pushnil(L);
		return 1;
	}
	lens = mysql_fetch_lengths(cur->res);
	lua_newtable(L);
	for (i = 0; i < cur->cols; i++) {
		lua_pushstring(L, fields[i].name);
		lua_pushlstring(L, row[i], lens[i]);
		lua_rawset(L, 2);
	}
	return 1;
}

int create_cursor(struct lua_State* L, MYSQL_RES* res) {
	struct cursor_t* cur = lua_newuserdata(L, sizeof(*cur));
	memset(cur, 0, sizeof(*cur));
	cur->res = res;
	cur->cols = mysql_num_fields(res);
    cur->rows = mysql_num_rows(res);
	if (luaL_newmetatable(L, CNAME_CURSOR)) {
		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
		lua_pushcfunction(L, cursor_close);
		lua_setfield(L, -2, "__gc");
		lua_pushcfunction(L, cursor_fetch_row);
		lua_setfield(L, -2, "fetchrow");
		lua_pushcfunction(L, cursor_close);
		lua_setfield(L, -2, "close");
	}
	lua_setmetatable(L, -2);
	return 1;
}

/*connection*/
int _select(struct lua_State* L) {
	struct mysql_t* my = (struct mysql_t*)luaL_checkudata (L, 1, CNAME_CONNECTION);
	luaL_argcheck (L, my != NULL, 1, "mysqlconnection expected");
	size_t tlen=0, slen=0;
	const char* tbl = luaL_checklstring(L, 2, &tlen);
	sql_reset(&my->sql);
	sql_cat(&my->sql, "SELECT ", 7, NULL);
	if (!lua_isnil(L, 3)) {
		const char* str = luaL_checklstring(L, 3, &slen);
		sql_cat(&my->sql, str, slen, NULL);
	} else
		sql_cat(&my->sql, "*", sizeof("*"), NULL);
	sql_cat(&my->sql, " FROM ", 6, NULL);
	sql_cat(&my->sql, tbl, tlen, NULL);
	if (!lua_isnil(L, 4)) {
		sql_cat(&my->sql, " WHERE ", 7, NULL);
		const char* str = luaL_checklstring(L, 4, &slen);
		sql_cat(&my->sql, str, slen, NULL);
	}
	//printf("sql %s\n", my->sql.ptr);
	if (mysql_real_query(my->conn, my->sql.ptr, my->sql.cur)) 
		return l_faildirect(L, mysql_error(my->conn));
	MYSQL_RES *res = mysql_store_result(my->conn);
	if (!res)
		return l_faildirect(L, mysql_error(my->conn));

	return create_cursor(L, res);
}

int _insert(struct lua_State* L) {
	struct mysql_t* my = (struct mysql_t*)luaL_checkudata (L, 1, CNAME_CONNECTION);
	luaL_argcheck (L, my != NULL, 1, "mysqlconnection expected");
	size_t tlen=0, flen=0, vlen=0;
	int cnt=0;
	const char* tbl = luaL_checklstring(L, 2, &tlen);
	if (!lua_istable(L, 3))
		return l_faildirect(L, "expected table in param 2");
	sql_reset(&my->sql);
	sql_reset(&my->fs);
	sql_reset(&my->vs);
	sql_cat(&my->sql, "INSERT ", 7, NULL);
	sql_cat(&my->sql, tbl, tlen, NULL);
	sql_cat(&my->sql, "(", 1, NULL);
	lua_pushnil(L);
	while(lua_next(L, -2) != 0) {
		if (lua_type(L, -2) != LUA_TSTRING) return l_faildirect(L, "expected string for key");
		if (lua_type(L, -1) != LUA_TSTRING && lua_type(L, -1) != LUA_TNUMBER) return l_faildirect(L, "expected string or number for value");
		const char* field = luaL_checklstring(L, -2, &flen);
		const char* value = luaL_checklstring(L, -1, &vlen);
		if (!field || !value) return l_faildirect(L, "field value exection");
		if (cnt++) {
			sql_cat(&my->fs, ",", 1, NULL);
			sql_cat(&my->vs, ",", 1, NULL);
		}
		sql_cat(&my->fs, "`", 1, NULL);
		sql_cat(&my->fs, field, flen, NULL);
		sql_cat(&my->fs, "`", 1, NULL);
		sql_cat(&my->vs, "'", 1, NULL);
		sql_cat(&my->vs, value, vlen, my->conn);
		sql_cat(&my->vs, "'", 1, NULL);
		lua_pop(L, 1);
	}
	sql_cat(&my->sql, my->fs.ptr, my->fs.cur, NULL);
	sql_cat(&my->sql, ")VALUES(", 8, NULL);
	sql_cat(&my->sql, my->vs.ptr, my->vs.cur, NULL);
	sql_cat(&my->sql, ")", 1, NULL);
	//printf("sql : %s\n", my->sql.ptr);
	if (mysql_real_query(my->conn, my->sql.ptr, my->sql.cur)) 
		return l_faildirect(L, mysql_error(my->conn));
	lua_pushnumber(L, mysql_affected_rows(my->conn));
	return 1;
}

int _update(struct lua_State* L) {
	struct mysql_t* my = (struct mysql_t*)luaL_checkudata (L, 1, CNAME_CONNECTION);
	luaL_argcheck (L, my != NULL, 1, "mysqlconnection expected");
	size_t tlen=0, clen=0, flen=0, vlen=0;
	int cnt=0;
	const char* tbl = luaL_checklstring(L, 2, &tlen);
	const char* con = NULL;
	if (!lua_istable(L, 3))
		return l_faildirect(L, "expected table in param 2");
	if (lua_isstring(L, 4)) {
		con = luaL_checklstring(L, 4, &clen);
	} else if (!lua_isnil(L, 4))
		return l_faildirect(L, "expected nil or string in param 3");
	sql_reset(&my->sql);
	sql_reset(&my->fs);
	sql_reset(&my->vs);
	sql_cat(&my->sql, "UPDATE ", 7, NULL);
	sql_cat(&my->sql, tbl, tlen, NULL);
	sql_cat(&my->sql, " SET ", 5, NULL);
	lua_pushvalue(L, 3);
	lua_pushnil(L);
	while(lua_next(L, -2) != 0) {
		if (lua_type(L, -2) != LUA_TSTRING) return l_faildirect(L, "expected string for key");
		if (lua_type(L, -1) != LUA_TSTRING && lua_type(L, -1) != LUA_TNUMBER) return l_faildirect(L, "expected string or number for value");
		const char* field = luaL_checklstring(L, -2, &flen);
		const char* value = luaL_checklstring(L, -1, &vlen);
		if (!field || !value) return l_faildirect(L, "field value exection");
		if (cnt++) {
			sql_cat(&my->fs, ",", 1, NULL);
			sql_cat(&my->vs, ",", 1, NULL);
		}
		sql_cat(&my->vs, "`", 1, NULL);
		sql_cat(&my->vs, field, flen, NULL);
		sql_cat(&my->vs, "`=", 2, NULL);
		sql_cat(&my->vs, "'", 1, NULL);
		sql_cat(&my->vs, value, vlen, my->conn);
		sql_cat(&my->vs, "'", 1, NULL);
		lua_pop(L, 1);
	}
	sql_cat(&my->sql, my->vs.ptr, my->vs.cur, NULL);
	if (con) {
		sql_cat(&my->sql, " WHERE ", 7, NULL);
		sql_cat(&my->sql, con, clen, NULL);
	}
	//printf("sql : %s\n", my->sql.ptr);
	if (mysql_real_query(my->conn, my->sql.ptr, my->sql.cur)) 
		return l_faildirect(L, mysql_error(my->conn));
	lua_pushnumber(L, mysql_affected_rows(my->conn));
	return 1;
}

int _delete(struct lua_State* L) {
	struct mysql_t* my = (struct mysql_t*)luaL_checkudata (L, 1, CNAME_CONNECTION);
	luaL_argcheck (L, my != NULL, 1, "mysqlconnection expected");
	size_t tlen=0, clen=0;
	const char* tbl = luaL_checklstring(L, 2, &tlen);
	const char* con = NULL;
	if (!lua_isnil(L, 3)) {
		con = luaL_checklstring(L, 3, &clen);
	}
	sql_reset(&my->sql);
	sql_cat(&my->sql, "DELETE FROM ", 12, NULL);
	sql_cat(&my->sql, tbl, tlen, NULL);
	if (con) {
		sql_cat(&my->sql, " WHERE ", 7, NULL);
		sql_cat(&my->sql, con, clen, NULL);
	}
	//printf("sql : %s\n", my->sql.ptr);
	if (mysql_real_query(my->conn, my->sql.ptr, my->sql.cur)) 
		return l_faildirect(L, mysql_error(my->conn));
	lua_pushnumber(L, mysql_affected_rows(my->conn));
	return 1;
}

int _connect_close(struct lua_State* L) {
	struct mysql_t* my = (struct mysql_t*)luaL_checkudata (L, 1, CNAME_CONNECTION);
	luaL_argcheck (L, my != NULL, 1, "mysqlconnection expected");
	if (!my->closed) {
		mysql_close(my->conn);
		mysql_library_end();
		my->closed = 1;
		if (my->sql.ptr) free(my->sql.ptr);
		if (my->vs.ptr)  free(my->vs.ptr);
		if (my->fs.ptr)  free(my->fs.ptr);
		printf("_connect_close\n");
	}
	lua_pushinteger(L, 0);
	return 1;
}

int _connect (struct lua_State *L) {
	const char *sourcename = luaL_checkstring(L, 2);
	const char *username = luaL_optstring(L, 3, NULL);
	const char *password = luaL_optstring(L, 4, NULL);
	const char *host = luaL_optstring(L, 5, NULL);
	const int port = luaL_optint(L, 6, 0);
	char value = 1;
	MYSQL* conn = mysql_init(NULL);
	if (conn == NULL)
		return l_faildirect(L, "out of memory");
	if (mysql_options(conn, MYSQL_OPT_RECONNECT, (char *)&value) || mysql_options(conn, MYSQL_SET_CHARSET_NAME, "utf8") || !mysql_real_connect(conn, host, username, password, sourcename, port, NULL, 0)) {
		char error_msg[128];
		strncpy (error_msg,  mysql_error(conn), 127);
		mysql_close (conn);
		return l_faildirect (L, error_msg);
	}
	struct mysql_t* c = lua_newuserdata(L, sizeof(*c));
	memset(c, 0, sizeof(*c));
	c->conn = conn;
	sql_init(&c->sql);
	sql_init(&c->fs);
	sql_init(&c->vs);
	if (luaL_newmetatable(L, CNAME_CONNECTION)) {
		lua_pushvalue(L, -1);
		lua_setfield(L, -2, "__index");
		lua_pushcfunction(L, _connect_close);
		lua_setfield(L, -2, "__gc");
		lua_pushcfunction(L, _insert);
		lua_setfield(L, -2, "insert");
		lua_pushcfunction(L, _update);
		lua_setfield(L, -2, "update");
		lua_pushcfunction(L, _select);
		lua_setfield(L, -2, "select");
		lua_pushcfunction(L, _delete);
		lua_setfield(L, -2, "delete");
		lua_pushcfunction(L, _connect_close);
		lua_setfield(L, -2, "close");
			
	}
	lua_setmetatable(L, -2);
	return 1;
}

int luaopen_lmysql(struct lua_State *L) {
	struct luaL_reg driver[] = {
		{"connect", _connect},
		{NULL, NULL},
	};
	luaL_openlib(L, "LUA_MYSQL", driver, 0);
	return 1;
}
