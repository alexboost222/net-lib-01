/* Smoke test of the C ABI: a server and a client in one process exchange a message. */
#define _DEFAULT_SOURCE
#include <stdio.h>
#include <string.h>
#include "../netlib.h"

#ifdef _WIN32
#include <windows.h>
static void sleep_ms(int ms) { Sleep(ms); }
#else
#include <unistd.h>
static void sleep_ms(int ms) { usleep(ms * 1000); }
#endif

static NetlibManager *create(int server) {
    NetlibConfig c;
    netlib_config_default(&c);
    c.protocol_id = 7;
    c.accept_incoming = (uint8_t)server;
    NetlibManager *m = NULL;
    if (netlib_manager_create(&c, "127.0.0.1:0", &m) != NETLIB_OK) return NULL;
    return m;
}

int main(void) {
    NetlibManager *server = create(1), *client = create(0);
    if (!server || !client) return 1;
    NetlibAddress a;
    netlib_local_address(server, &a);
    char target[32];
    snprintf(target, sizeof target, "127.0.0.1:%u", a.port);
    uint64_t peer = 0;
    if (netlib_connect(client, target, NULL, 0, &peer) != NETLIB_OK) return 2;
    int connected = 0, received = 0;
    for (int i = 0; i < 400 && !received; i++) {
        NetlibManager *ms[2] = {server, client};
        for (int k = 0; k < 2; k++) {
            NetlibEvent ev;
            netlib_poll(ms[k]);
            while (netlib_next_event(ms[k], &ev) == 1) {
                if (ev.kind == NETLIB_EVENT_CONNECT_REQUEST) netlib_accept(ms[k], ev.peer);
                if (ev.kind == NETLIB_EVENT_CONNECTED && k == 1) connected = 1;
                if (ev.kind == NETLIB_EVENT_MESSAGE && ev.data_len == 5 && memcmp(ev.data, "hello", 5) == 0) received = 1;
            }
            if (k == 1 && connected && i % 10 == 0) netlib_send(client, peer, 0, (const uint8_t *)"hello", 5);
            netlib_flush(ms[k]);
        }
        sleep_ms(5);
    }
    netlib_manager_destroy(client);
    netlib_manager_destroy(server);
    printf("netlib %s: %s\n", netlib_version(), received ? "ok" : "FAILED");
    return received ? 0 : 3;
}
