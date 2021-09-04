#include "rdma-client-fun.h"

static const int RDMA_BUFFER_SIZE = 1024;

static void build_context(struct ibv_context *verbs);
static void build_qp_attr(struct ibv_qp_init_attr *qp_attr);
static void on_completion(struct ibv_wc *);
static void read_remote(struct connection *conn, int HoB, uint64_t offset);
static void write_remote(struct connection *conn, int HoB, uint64_t offset);
static void * poll_send_cq(void *);
static void * poll_recv_cq(void *);
static void post_receives(struct connection *conn);
static void register_memory(struct connection *conn);

static struct context *s_ctx = NULL;

void die(const char *reason)
{
  	fprintf(stderr, "%s\n", reason);
 	exit(EXIT_FAILURE);
}

void build_connection(struct rdma_cm_id *id)
{
  	struct connection *conn;
  	struct ibv_qp_init_attr qp_attr;

 	build_context(id->verbs);
  	build_qp_attr(&qp_attr);

  	TEST_NZ(rdma_create_qp(id, s_ctx->pd, &qp_attr));

  	id->context = conn = (struct connection *)malloc(sizeof(struct connection));

  	conn->id = id;
  	conn->qp = id->qp;

  	register_memory(conn);
  	post_receives(conn);
}

void build_context(struct ibv_context *verbs)
{
  	if (s_ctx) 
	{
    		if (s_ctx->ctx != verbs)
      			die("cannot handle events in more than one context.");

    		return;
  	}

  	s_ctx = (struct context *)malloc(sizeof(struct context));

  	s_ctx->ctx = verbs;

  	TEST_Z(s_ctx->pd = ibv_alloc_pd(s_ctx->ctx));
  	TEST_Z(s_ctx->send_comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  	TEST_Z(s_ctx->recv_comp_channel = ibv_create_comp_channel(s_ctx->ctx));
  	TEST_Z(s_ctx->send_cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->send_comp_channel, 0)); /* cqe=10 is arbitrary */
  	TEST_Z(s_ctx->recv_cq = ibv_create_cq(s_ctx->ctx, 10, NULL, s_ctx->recv_comp_channel, 0)); /* cqe=10 is arbitrary */

  	TEST_NZ(ibv_req_notify_cq(s_ctx->send_cq, 0));
	TEST_NZ(ibv_req_notify_cq(s_ctx->recv_cq, 0));
  	TEST_NZ(pthread_create(&s_ctx->send_cq_poller_thread, NULL, poll_send_cq, NULL));
	TEST_NZ(pthread_create(&s_ctx->recv_cq_poller_thread, NULL, poll_recv_cq, NULL));
}

void build_params(struct rdma_conn_param *params)
{
  	memset(params, 0, sizeof(*params));

  	params->initiator_depth = params->responder_resources = 1;
  	params->rnr_retry_count = 7; /* infinite retry */
}

void build_qp_attr(struct ibv_qp_init_attr *qp_attr)
{
  	memset(qp_attr, 0, sizeof(*qp_attr));

  	qp_attr->send_cq = s_ctx->send_cq;
  	qp_attr->recv_cq = s_ctx->recv_cq;
  	qp_attr->qp_type = IBV_QPT_RC;

  	qp_attr->cap.max_send_wr = 10;
  	qp_attr->cap.max_recv_wr = 10;
  	qp_attr->cap.max_send_sge = 1;
  	qp_attr->cap.max_recv_sge = 1;
}

void destroy_connection(void *context)
{
  	struct connection *conn = (struct connection *)context;

  	rdma_destroy_qp(conn->id);

  	ibv_dereg_mr(conn->send_mr);
  	ibv_dereg_mr(conn->recv_mr);
  	ibv_dereg_mr(conn->rdma_local_mr);
  	ibv_dereg_mr(conn->rdma_remote_mr);

  	free(conn->send_msg);
  	free(conn->recv_msg);
  	free(conn->rdma_local_region);
  	free(conn->rdma_remote_region);

  	rdma_destroy_id(conn->id);

  	free(conn);
}

void on_completion(struct ibv_wc *wc)
{
	struct connection *conn = (struct connection *)(uintptr_t)wc->wr_id;

  	if (wc->status != IBV_WC_SUCCESS)
    		die("on_completion: status is not IBV_WC_SUCCESS.");

  	if (wc->opcode == IBV_WC_RECV) 
	{
		printf("recv completed successfully.\n");

		post_receives(conn);

		if (conn->recv_msg->type == CONN) 
		{
			//printf("start to establish connection.\n");
			
			conn->send_msg->type = REQ_HASHTABLE;
			
			send_message(conn);
    		}
		else if (conn->recv_msg->type == HASHTABLE_MR)
		{
			//printf("get hashtable address.\n");
			memcpy(&conn->hashtable_mr,&conn->recv_msg->data.mr,sizeof(struct ibv_mr));
			//printf("hashtable address is %p.\n",conn->hashtable_mr.addr);

                        conn->send_msg->type = REQ_BUCKET;

                        send_message(conn);
		}
		else if (conn->recv_msg->type == BUCKET_MR)
		{
			//printf("get bucketDocker address.\n");
			memcpy(&conn->bucketDocker_mr,&conn->recv_msg->data.mr,sizeof(struct ibv_mr));
			//printf("bucketDocker address is %p.\n",conn->bucketDocker_mr.addr);

                        conn->send_msg->type = DONE;

                        send_message(conn);
		}
  	} 
	else if (wc->opcode == IBV_WC_SEND)
	{
		printf("send completed successfully.\n");

		if(conn->send_msg->type == DONE)
		{
			//printf("going to disconnect.\n");

			//rdma_disconnect(conn->id);
			__sync_fetch_and_add(&conn->flag,1);
		}
	}
	else if (wc->opcode == IBV_WC_RDMA_READ)
  	{
  		printf("read is completion.\n");
		
		//rdma_disconnect(conn->id);
		gettime(&end1);
                float cost1 = timediff_us(start,end1);

                printf("read cost time is %f us.\n",cost1);
  	}
	else if(wc->opcode == IBV_WC_RDMA_WRITE)
  	{
  		printf("write is completion.\n");
		gettime(&end3);
		float cost2 = timediff_us(end2,end3);

		printf("write cost time is %f us.\n",cost2);

  	}
	else if (wc->opcode == IBV_WC_RECV_RDMA_WITH_IMM)
	{
		printf("write with imm completion.\n");

		uint32_t imm = ntohl(wc->imm_data);
		
		printf("imm is %d.\n",imm);	
	}

}

void read_remote(struct connection * conn, int HoB, uint64_t offset)
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;
	
	memset(&wr, 0, sizeof(wr));

	wr.wr_id = (uintptr_t)conn;
	wr.opcode = IBV_WR_RDMA_READ;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.wr.rdma.remote_addr = ((HoB == 0) ? (uintptr_t)conn->hashtable_mr.addr : ((uintptr_t)conn->bucketDocker_mr.addr + offset));
	wr.wr.rdma.rkey = ((HoB == 0) ? conn->hashtable_mr.rkey : conn->bucketDocker_mr.rkey);

	memset(conn->rdma_remote_region,0,RDMA_BUFFER_SIZE);

	sge.addr = (uintptr_t)conn->rdma_remote_region;
	sge.length = ((HoB == 0) ? sizeof(struct hashTable) : sizeof(struct bucket));
        sge.lkey = conn->rdma_remote_mr->lkey;

	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void write_remote(struct connection * conn, int HoB, uint64_t offset)
{
        struct ibv_send_wr wr, *bad_wr = NULL;
        struct ibv_sge sge;

        memset(&wr, 0, sizeof(wr));
        wr.wr_id = (uintptr_t)conn;
        wr.opcode = IBV_WR_RDMA_WRITE;
        wr.sg_list = &sge;
        wr.num_sge = 1;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.wr.rdma.remote_addr = ((HoB == 0) ? ((uintptr_t)conn->hashtable_mr.addr + offset) : ((uintptr_t)conn->bucketDocker_mr.addr + offset));
        wr.wr.rdma.rkey = ((HoB == 0) ? conn->hashtable_mr.rkey : conn->bucketDocker_mr.rkey);

        sge.addr = (uintptr_t)conn->rdma_local_region;
        sge.length = ((HoB == 0) ? sizeof(struct hashTable) : sizeof(struct bucket));
        sge.lkey = conn->rdma_local_mr->lkey;

        TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}

void write_with_imm(struct connection * conn, uint32_t imm)
{
	struct ibv_send_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	memset(&wr,0,sizeof(struct ibv_send_wr));
	wr.wr_id = (uintptr_t)conn;
	wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
	wr.sg_list = &sge;
	wr.num_sge = 1;
	wr.send_flags = IBV_SEND_SIGNALED;
	wr.imm_data = imm;

	wr.wr.rdma.remote_addr = (uintptr_t)conn->hashtable_mr.addr;
	wr.wr.rdma.rkey = conn->hashtable_mr.rkey;

	sge.addr = (uintptr_t)conn->rdma_local_region;
	sge.length = sizeof(struct hashTable);
	sge.lkey = conn->rdma_local_mr->lkey;
	
	//gettime(&start);
	TEST_NZ(ibv_post_send(conn->qp,&wr,&bad_wr));

	//post_receives(conn);
}

void on_connect(void *context)
{
	printf("connection completed.\n");
	struct connection *conn = (struct connection *)context;
	
//	struct hashTable * ht2 = (struct hashTable *)conn->rdma_local_region;
//	ht2->size = 1;
//	uint32_t imm = htonl(11);
//	//gettime(&start);
//	write_with_imm(conn,imm);

	gettime(&start);
	struct hashTable * ht1 = (struct hashTable *)conn->rdma_remote_region;
	read_remote(conn,0,0);
	printf("ht1->size: %d.\n",ht1->size);

	gettime(&end2);
	struct hashTable * ht2 = (struct hashTable *)conn->rdma_local_region;
	ht2->size = ht1->size + 1;
	write_remote(conn,0,0);
	
	gettime(&end4);
	char *key1 = "user6284781860667377211";
	uint64_t offset1 = (murmurhash(key1,strlen(key1),SEED) % HASHTABLESIZE) * sizeof(struct bucket);
	struct bucket *bucket1 = (struct bucket *)conn->rdma_local_region;
	strcpy(bucket1->key,key1);
	bucket1->valuePtr = (void *)0x1a2b3c;
	bucket1->valueLen = 10;
	write_remote(conn,1,offset1);
	float cost3 = timediff_us(end2,end4);
	printf("time between two write is %f.\n",cost3);

	gettime(&end5);
	struct hashTable * ht3 = (struct hashTable *)conn->rdma_remote_region;
	read_remote(conn,0,0);
	float cost4 = timediff_us(start,end5);
        printf("time between two read is %f.\n",cost4);
			
	printf("ht3->size: %d.\n",ht3->size);
}

void * poll_send_cq(void *ctx)
{
	struct ibv_cq *cq;
  	struct ibv_wc wc;

  	while (1) 
	{
		TEST_NZ(ibv_get_cq_event(s_ctx->send_comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (ibv_poll_cq(cq, 1, &wc))
			on_completion(&wc);
  	}	

	return NULL;
}

void * poll_recv_cq(void *ctx)
{
  	struct ibv_cq *cq;
 	struct ibv_wc wc;

  	while (1) 
	{
		TEST_NZ(ibv_get_cq_event(s_ctx->recv_comp_channel, &cq, &ctx));
		ibv_ack_cq_events(cq, 1);
		TEST_NZ(ibv_req_notify_cq(cq, 0));

		while (ibv_poll_cq(cq, 1, &wc))
      			on_completion(&wc);
  	}

  	return NULL;
}


void post_receives(struct connection *conn)
{
	struct ibv_recv_wr wr, *bad_wr = NULL;
	struct ibv_sge sge;

	wr.wr_id = (uintptr_t)conn;
	wr.next = NULL;
	wr.sg_list = &sge;
	wr.num_sge = 1;

	sge.addr = (uintptr_t)conn->recv_msg;
	sge.length = sizeof(struct message);
	sge.lkey = conn->recv_mr->lkey;

	TEST_NZ(ibv_post_recv(conn->qp, &wr, &bad_wr));
}

void register_memory(struct connection *conn)
{
	conn->send_msg = malloc(sizeof(struct message));
	conn->recv_msg = malloc(sizeof(struct message));

	conn->rdma_local_region = malloc(RDMA_BUFFER_SIZE);
	conn->rdma_remote_region = malloc(RDMA_BUFFER_SIZE);

	conn->flag = 0;
	TEST_Z(conn->send_mr = ibv_reg_mr(s_ctx->pd, conn->send_msg, sizeof(struct message), 0));
	TEST_Z(conn->recv_mr = ibv_reg_mr(s_ctx->pd, conn->recv_msg, sizeof(struct message), IBV_ACCESS_LOCAL_WRITE));	
	TEST_Z(conn->rdma_local_mr = ibv_reg_mr(s_ctx->pd, conn->rdma_local_region, RDMA_BUFFER_SIZE, (IBV_ACCESS_LOCAL_WRITE |IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE)));
	TEST_Z(conn->rdma_remote_mr = ibv_reg_mr(s_ctx->pd, conn->rdma_remote_region, RDMA_BUFFER_SIZE, (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ)));
}

void send_message(struct connection *conn)
{
  	struct ibv_send_wr wr, *bad_wr = NULL;
  	struct ibv_sge sge;

  	memset(&wr, 0, sizeof(wr));

 	wr.wr_id = (uintptr_t)conn;
  	wr.opcode = IBV_WR_SEND;
  	wr.sg_list = &sge;
  	wr.num_sge = 1;
  	wr.send_flags = IBV_SEND_SIGNALED;

  	sge.addr = (uintptr_t)conn->send_msg;
  	sge.length = sizeof(struct message);
  	sge.lkey = conn->send_mr->lkey;

  	TEST_NZ(ibv_post_send(conn->qp, &wr, &bad_wr));
}
