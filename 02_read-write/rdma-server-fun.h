#ifndef RDMA_SERVER_FUN_H
#define RDMA_SERVER_FUN_H

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <rdma/rdma_cma.h>

#define TEST_NZ(x) do { if ( (x)) die("error: " #x " failed (returned non-zero)." ); } while (0)
#define TEST_Z(x)  do { if (!(x)) die("error: " #x " failed (returned zero/null)."); } while (0)

struct context {
  struct ibv_context *ctx;
  struct ibv_pd *pd;
  struct ibv_cq *send_cq;
struct ibv_cq * recv_cq;
  struct ibv_comp_channel *send_comp_channel;
  struct ibv_comp_channel *recv_comp_channel;

  pthread_t send_cq_poller_thread;
  pthread_t recv_cq_poller_thread;
};

struct connection {
  struct rdma_cm_id *id;
  struct ibv_qp *qp;

  //int connected;

  struct ibv_mr *recv_mr;
  struct ibv_mr *send_mr;
  struct ibv_mr *rdma_local_mr;
  struct ibv_mr *rdma_remote_mr;

  struct ibv_mr peer_mr;

  struct message *recv_msg;
  struct message *send_msg;

  char *rdma_local_region;
  char *rdma_remote_region;

  enum {
    SS_INIT,
    SS_MR_SENT,
    SS_RDMA_SENT,
    SS_DONE_SENT
  } send_state;

  enum {
    RS_INIT,
    RS_MR_RECV,
    RS_DONE_RECV
  } recv_state;
};

struct message {
  enum {
    MSG_MR,
    MSG_DONE
  } type;

  union {
    struct ibv_mr mr;
  } data;
};

#define KEYSIZE 20
#define HASHTABLESIZE 20000
#define SEED 0x9c8d7e6f

struct bucket
{
        char key[KEYSIZE];
        void * valuePtr;
        int valueLen;
};

//struct bucket * bucketDocker1 = NULL;

struct hashTable
{
        int size;
        int capacity;
        struct bucket * array;
};

//struct hashTable * hashtable1 = NULL;

void die(const char *reason);

void build_connection(struct rdma_cm_id *id);
void build_params(struct rdma_conn_param *params);
void destroy_connection(void *context);
void * get_local_message_region(void *context);
void on_connect(void *context);
void send_mr(void *context);
void send_done(void *context);

#endif
