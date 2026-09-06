/* Minimal MCP ping server in pure C — transport-floor reference.
 * Handles just enough newline-delimited JSON-RPC to measure per-call
 * transport cost: initialize, ping. Everything else gets -32601.
 * id extraction is strstr/atoi — a bench tool, not a JSON parser. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static long extract_id(const char *line) {
    const char *p = strstr(line, "\"id\":");
    if (!p) return -1;
    return strtol(p + 5, NULL, 10);
}

int main(void) {
    static char line[65536];
    /* stdout must be unbuffered: every reply must flush immediately */
    setvbuf(stdout, NULL, _IONBF, 0);
    while (fgets(line, sizeof line, stdin)) {
        long id = extract_id(line);
        if (id < 0) continue;  /* notification */
        if (strstr(line, "\"initialize\""))
            printf("{\"jsonrpc\":\"2.0\",\"id\":%ld,\"result\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{\"tools\":{}},\"serverInfo\":{\"name\":\"cref\",\"version\":\"0\"}}}\n", id);
        else if (strstr(line, "\"ping\""))
            printf("{\"jsonrpc\":\"2.0\",\"id\":%ld,\"result\":{}}\n", id);
        else
            printf("{\"jsonrpc\":\"2.0\",\"id\":%ld,\"error\":{\"code\":-32601,\"message\":\"nf\"}}\n", id);
    }
    return 0;
}
