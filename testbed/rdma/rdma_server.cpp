/*
 * A server on remote memory node
 * Handle both TCP and RDMA message for remote work
 */
#include "rdma_server.h"
#include <pthread.h>
#include <string>

using namespace std;

/* A tcp server to listen operation message from local node */
void *tcp_thread(void *args) {
  int sock = socket(AF_INET, SOCK_STREAM, 0);
  struct sockaddr_in addr;
  addr.sin_port = htons(53101);
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = tcp_server_addr;
  if (bind(sock, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
    perror("bind failed");
    return NULL;
  }
  if (listen(sock, 1) < 0) {
    perror("listen failed");
    return NULL;
  }
  int client = accept(sock, NULL, NULL);
  if (client != -1) {
    printf("TCP Connection: OK\n");
  }
  else {
    printf("TCP Connection: ERROR\n");
    return NULL;
  }
  
  char msg[128];
  int ret;

  while(true) {
    ret = recv(client, msg, sizeof(msg), 0);
    if (ret > 0) {
      string strmsg = string(msg);
      if (strmsg == "expand") {
        // Get into the state for expansion
        expand();
      }
      else if (strmsg == "zero") {
        // Set whole remote memory to zero
        set_mr_zero();
      }
      else if (strmsg == "over") {
        // Close rdma server
        rdma_server_close();
        break;
      }
    }
    else if (ret < 0) {
      printf("TCP ERROR\n");
      break;
    }
    else {
      printf("TCP Client closed\n");
      break;
    }
  }

  close(client);
  close(sock);

  exit(0);
}

int main(int argc, char **argv) {
  int ret = 0;

  // Initialize rdma server
  ret = rdma_server_init(argc, argv);
  if (ret) {
    return ret;
  }

  pthread_t t_tcp;
  pthread_create(&t_tcp, NULL, tcp_thread, NULL);

  // We also provide an operating platform that supports simple commands
  char cmd[64];
  while (1) {
    //expand();
    printf("You can input \"expand\" or \"quit\".\n");
    if (fgets(cmd, 64, stdin) == NULL)
      break;
    if (strcmp(cmd, "quit\n") == 0)
      break;
    if (strcmp(cmd, "expand\n") == 0)
      expand();
  }

  ret = rdma_server_close();
  return ret;
}
