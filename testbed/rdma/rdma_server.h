/*
 * This is a RDMA server side code.
 *
 */

#include "rdma_common.h"
#include <chrono>
#include <vector>

/* These are the RDMA resources needed to setup an RDMA connection */
/* Event channel, where connection management (cm) related events are relayed */
static struct rdma_event_channel * cm_event_channel;
static struct rdma_cm_id *cm_server_id;
static std::vector<rdma_cm_id *> cm_client_id;
static std::vector<ibv_pd *> pd;
static std::vector<ibv_comp_channel *> io_completion_channel;
static std::vector<ibv_cq *> cq;
static std::vector<ibv_qp_init_attr> qp_init_attr;
static std::vector<ibv_qp *> client_qp;
/* RDMA memory resources */
static std::vector<ibv_mr *> client_metadata_mr;
static std::vector<void *> server_buffer;
static std::vector<std::vector<ibv_mr *> > server_buffer_mr, server_metadata_mr;
static std::vector<rdma_buffer_attr> client_metadata_attr;
static std::vector<std::vector<rdma_buffer_attr> > server_metadata_attr;
/* These are exchange infomation related resources */
static std::vector<ibv_recv_wr> client_recv_wr; 
static std::vector<ibv_recv_wr *> bad_client_recv_wr;
static std::vector<ibv_send_wr> server_send_wr;
static std::vector<ibv_send_wr *> bad_server_send_wr;
static std::vector<ibv_sge> client_recv_sge, server_send_sge;
/* the number of RDMA connections */
static int connect_num = 1;

/* record tcp server address */
static int tcp_server_addr = 0;

/* Set all entries in memory regions to zero */
static inline int set_mr_zero() {
  int ret = -1;
  size_t sz = server_metadata_attr[0][0].length;

  for (auto &i: server_buffer) {
    memset(i, 0, sz);
  }

  ret = 0;
  return ret;
}

/* Extend remote memory with info from client */
static inline int expand() {
  struct ibv_wc wc;
  int ret = -1;
	int n = 1;

  client_recv_sge[connect_num-1].addr = (uint64_t)client_metadata_mr[connect_num-1]->addr;
  client_recv_sge[connect_num-1].length = client_metadata_mr[connect_num-1]->length;
  client_recv_sge[connect_num-1].lkey = client_metadata_mr[connect_num-1]->lkey;
  /* Now we link this SGE to the work request (WR) */
  bzero(&client_recv_wr[connect_num-1], sizeof(client_recv_wr[connect_num-1]));
  client_recv_wr[connect_num-1].sg_list = &client_recv_sge[connect_num-1];
  client_recv_wr[connect_num-1].num_sge = 1; // only one SGE
  ret = ibv_post_recv(client_qp[connect_num-1] /* which QP */,
                      &client_recv_wr[connect_num-1] /* receive work request*/,
                      &bad_client_recv_wr[connect_num-1] /* error WRs */);
  if (ret) {
    rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
    return ret;
  }
  ret = process_work_completion_events(io_completion_channel[connect_num-1], &wc, 1);
  if (ret != 1) {
    rdma_error("Failed to receive an expansion info, ret = %d \n", ret);
    return ret;
  }

	/* Get expansion info n from client
	 * Then we could have new_sz = old_sz*n,
	 * and allocate new_sz-old_sz blocks for expansion
	 */
  n = client_metadata_attr[connect_num-1].length;
	//printf("%d\n", n);
  int old_sz = server_metadata_mr.size();
  int new_sz = n*old_sz;

  for (int i = 0; i < connect_num; ++i) {

  	for (int j = old_sz; j < new_sz; ++j) {
			if (i == 0) {
				server_metadata_attr.push_back({});
  			server_buffer_mr.push_back({});
  			server_metadata_mr.push_back({});
			}

	  	server_metadata_attr[j].push_back({});
  	  server_buffer_mr[j].push_back({});
      server_metadata_mr[j].push_back({});

  	  server_metadata_attr[j][i].length = server_metadata_attr[0][i].length;
  	  if (i == 0) {
				// only allocate once
  			server_buffer.push_back({});
  			server_buffer[j] = calloc(1, server_metadata_attr[j][0].length);
  			if (!server_buffer[j]) {
  			  rdma_error("Server failed to allocate buffer \n");
  			  return -ENOMEM;
  			}
  	  }
			
  	  server_buffer_mr[j][i] = rdma_buffer_register(
  	    pd[i] /* which protection domain */,
  	    server_buffer[j],
  	    server_metadata_attr[j][i].length /* what size to allocate */,
  	    (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
  	    IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
  	  if (!server_buffer_mr[j][i]) {
  	    rdma_error("Server failed to create a buffer \n");
  	    /* we assume that it is due to out of memory error */
  	    return -ENOMEM;
  	  }

  	  server_metadata_attr[j][i].address = (uint64_t)server_buffer_mr[j][i]->addr;
  	  server_metadata_attr[j][i].length = (uint64_t)server_buffer_mr[j][i]->length;
  	  server_metadata_attr[j][i].stag.local_stag = (uint32_t)server_buffer_mr[j][i]->lkey;
  	  server_metadata_mr[j][i] = rdma_buffer_register(
  	      pd[i] /* which protection domain*/,
  	      &server_metadata_attr[j][i] /* which memory to register */,
  	      sizeof(server_metadata_attr[j][i]) /* what is the size of memory */,
  	      IBV_ACCESS_LOCAL_WRITE /* what access permission */);
  	  if (!server_metadata_mr[j][i]) {
  	    rdma_error("Server failed to create to hold server metadata \n");
  	    /* we assume that this is due to out of memory error */
  	    return -ENOMEM;
  	  }

  	  server_send_sge[i].addr = (uint64_t)&server_metadata_attr[j][i];
  	  server_send_sge[i].length = sizeof(server_metadata_attr[j][i]);
  	  server_send_sge[i].lkey = server_metadata_mr[j][i]->lkey;
  	  /* now we link this sge to the send request */
  	  bzero(&server_send_wr[i], sizeof(server_send_wr[i]));
  	  server_send_wr[i].sg_list = &server_send_sge[i];
  	  server_send_wr[i].num_sge = 1;          // only 1 SGE element in the array
  	  server_send_wr[i].opcode = IBV_WR_SEND; // This is a send request
  	  server_send_wr[i].send_flags = IBV_SEND_SIGNALED; // We want to get notification
  	  /* This is a fast data path operation. Posting an I/O request */
  	  ret = ibv_post_send(
  	      client_qp[i] /* which QP */,
  	      &server_send_wr[i] /* Send request that we prepared before */, &bad_server_send_wr[i] /* In case of error, this will contain failed requests */);
  	  if (ret) {
  	    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
  	    return -errno;
  	  }
  	  /* We check for completion notification */
  	  ret = process_work_completion_events(io_completion_channel[i], &wc, 1);
  	  if (ret != 1) {
  	    rdma_error("Failed to send server metadata, ret = %d \n", ret);
  	    return ret;
  	  }
  	  debug("Local buffer metadata has been sent to the client \n");

	  	if (i == 0) {
				// only copy once
				memcpy((void*)server_metadata_attr[j][i].address,
  	         	 (void*)server_metadata_attr[j%old_sz][i].address,
  	         	 server_metadata_attr[j][i].length);
				printf("tid: %d, MR[%d]:\n", i, j);
  			show_rdma_buffer_attr(&server_metadata_attr[j][i]);	
			}
  	}
  }

  return 0;
}


/* When we call this function cm_client_id must be set to a valid identifier.
 * This is where, we prepare client connection before we accept it. This
 * mainly involve pre-posting a receive buffer to receive client side
 * RDMA credentials
 */
static int setup_client_resources(int i) {
  int ret = -1;

  // for NO.i connection
  if (!cm_client_id[i]) {
    rdma_error("Client id is still NULL \n");
    return -EINVAL;
  }
  /* We have a valid connection identifier, lets start to allocate
   * resources. We need:
   * 1. Protection Domains (PD)
   * 2. Memory Buffers
   * 3. Completion Queues (CQ)
   * 4. Queue Pair (QP)
   * Protection Domain (PD) is similar to a "process abstraction"
   * in the operating system. All resources are tied to a particular PD.
   * And accessing recourses across PD will result in a protection fault.
   */
  pd.push_back({});
  pd[i] = ibv_alloc_pd(cm_client_id[i]->verbs
  		/* verbs defines a verb's provider, 
  		 * j.e an RDMA device where the incoming 
  		 * client connection came */);
  if (!pd[i]) {
    rdma_error("Failed to allocate a protection domain errno: %d\n", -errno);
    return -errno;
  }
  debug("A new protection domain is allocated at %p \n", pd[i]);
  /* Now we need a completion channel, were the I/O completion
   * notifications are sent. Remember, this is different from connection
   * management (CM) event notifications.
   * A completion channel is also tied to an RDMA device, hence we will
   * use cm_client_id->verbs.
   */
  io_completion_channel.push_back({});
  io_completion_channel[i] = ibv_create_comp_channel(cm_client_id[i]->verbs);
  if (!io_completion_channel[i]) {
    rdma_error("Failed to create an I/O completion event channel, %d\n",
               -errno);
    return -errno;
  }
  debug("An I/O completion event channel is created at %p \n",
        io_completion_channel[i]);
  /* Now we create a completion queue (CQ) where actual I/O
   * completion metadata is placed. The metadata is packed into a structure
   * called struct ibv_wc (wc = work completion). ibv_wc has detailed
   * information about the work completion. An I/O request in RDMA world
   * is called "work" ;)
   */
  cq.push_back({});
  cq[i] = ibv_create_cq(cm_client_id[i]->verbs /* which device*/,
                     CQ_CAPACITY /* maximum capacity*/,
                     NULL /* user context, not used here */,
                     io_completion_channel[i] /* which IO completion channel */,
                     0 /* signaling vector, not used here*/);
  if (!cq[i]) {
    rdma_error("Failed to create a completion queue (cq), errno: %d\n", -errno);
    return -errno;
  }
  debug("Completion queue (CQ) is created at %p with %d elements \n", cq[i],
        cq[i]->cqe);
  /* Ask for the event for all activities in the completion queue*/
  ret = ibv_req_notify_cq(cq[i] /* on which CQ */,
                          0 /* 0 = all event type, no filter*/);
  if (ret) {
    rdma_error("Failed to request notifications on CQ errno: %d \n", -errno);
    return -errno;
  }
  /* Now the last step, set up the queue pair (send, recv) queues and their
   * capacity. The capacity here is define statically but this can be probed
   * from the device. We just use a small number as defined in rdma_common.h */
  qp_init_attr.push_back({});
  bzero(&qp_init_attr[i], sizeof qp_init_attr[i]);
  qp_init_attr[i].cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
  qp_init_attr[i].cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
  qp_init_attr[i].cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
  qp_init_attr[i].cap.max_send_wr = MAX_WR;   /* Maximum send posting capacity */
  qp_init_attr[i].qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
  /* We use same completion queue, but one can use different queues */
  qp_init_attr[i].recv_cq =
      cq[i]; /* Where should I notify for receive completion operations */
  qp_init_attr[i].send_cq =
      cq[i]; /* Where should I notify for send completion operations */
  /*Lets create a QP */
  ret = rdma_create_qp(cm_client_id[i] /* which connection id */,
                       pd[i] /* which protection domain*/,
                       &qp_init_attr[i] /* Initial attributes */);
  if (ret) {
    rdma_error("Failed to create QP due to errno: %d\n", -errno);
    return -errno;
  }
  /* Save the reference for handy typing but is not required */
  client_qp.push_back({});
  client_qp[i] = cm_client_id[i]->qp;
  debug("Client QP created at %p\n", client_qp[i]);
  
  return ret;
}

/* Starts an RDMA server by allocating basic connection resources */
static int start_rdma_server(struct sockaddr_in *server_addr) {
  int ret = -1;
  /*  Open a channel used to report asynchronous communication event */
  cm_event_channel = rdma_create_event_channel();
  if (!cm_event_channel) {
    rdma_error("Creating cm event channel failed with errno : (%d)", -errno);
    return -errno;
  }
  debug("RDMA CM event channel is created successfully at %p \n",
        cm_event_channel);
  /* rdma_cm_id is the connection identifier (like socket) which is used
   * to define an RDMA connection.
   */
  ret = rdma_create_id(cm_event_channel, &cm_server_id, NULL, RDMA_PS_TCP);
  if (ret) {
    rdma_error("Creating server cm id failed with errno: %d ", -errno);
    return -errno;
  }
  debug("A RDMA connection id for the server is created \n");
  /* Explicit binding of rdma cm id to the socket credentials */
  ret = rdma_bind_addr(cm_server_id, (struct sockaddr *)server_addr);
  if (ret) {
    rdma_error("Failed to bind server address, errno: %d \n", -errno);
    return -errno;
  }
  debug("Server RDMA CM id is successfully binded \n");
  /* Now we start to listen on the passed IP and port. However unlike
   * normal TCP listen, this is a non-blocking call. When a new client is
   * connected, a new connection management (CM) event is generated on the
   * RDMA CM event channel from where the listening id was created. Here we
   * have only one channel, so it is easy. */
  ret = rdma_listen(cm_server_id,
                    connect_num); /* backlog = 8 clients, same as TCP, see man listen*/
  if (ret) {
    rdma_error("rdma_listen failed to listen on server address, errno: %d ",
               -errno);
    return -errno;
  }
  printf("Server is listening successfully at: %s , port: %d \n",
         inet_ntoa(server_addr->sin_addr), ntohs(server_addr->sin_port));
  /* now, we expect a client to connect and generate a
   * RDMA_CM_EVNET_CONNECT_REQUEST We wait (block) on the connection management
   * event channel for the connect event.
   */
  return ret;

}

static int rdma_server_listen(int i) {
  struct rdma_cm_event * cm_event = NULL;
  int ret = -1;

  // for NO.i connection
  ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_CONNECT_REQUEST,
                              &cm_event);
  if (ret) {
    rdma_error("Failed to get cm event, ret = %d \n", ret);
    return ret;
  }
  /* Much like TCP connection, listening returns a new connection identifier
   * for newly connected client. In the case of RDMA, this is stored in id
   * field. For more details: man rdma_get_cm_event
   */
  cm_client_id.push_back({});
  cm_client_id[i] = cm_event->id;
  /* now we acknowledge the event. Acknowledging the event free the resources
   * associated with the event structure. Hence any reference to the event
   * must be made before acknowledgment. Like, we have already saved the
   * client id from "id" field before acknowledging the event.
   */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    rdma_error("Failed to acknowledge the cm event errno: %d \n", -errno);
    return -errno;
  }
  debug("A new RDMA client connection id is stored at %p\n", cm_client_id);
  
  return ret;
}

/* Pre-posts a receive buffer and accepts an RDMA client connection */
static int accept_client_connection(int i) {
  int ret = -1;

  // for NO.i connection

  struct rdma_conn_param conn_param;
  struct rdma_cm_event *cm_event = NULL;
  struct sockaddr_in remote_sockaddr;

  if (!cm_client_id[i] || !client_qp[i]) {
    rdma_error("Client resources are not properly setup\n");
    return -EINVAL;
  }
  /* we prepare the receive buffer in which we will receive the client
   * metadata*/
  client_metadata_mr.push_back({});
  client_metadata_attr.push_back({});
  client_metadata_mr[i] = rdma_buffer_register(
      pd[i] /* which protection domain */, &client_metadata_attr[i] /* what memory */,
      sizeof(client_metadata_attr[i]) /* what length */,
      (IBV_ACCESS_LOCAL_WRITE) /* access permissions */);
  if (!client_metadata_mr[i]) {
    rdma_error("Failed to register client attr buffer\n");
    // we assume ENOMEM
    return -ENOMEM;
  }
  /* We pre-post this receive buffer on the QP. SGE credentials is where we
   * receive the metadata from the client */
  client_recv_sge.push_back({});
  client_recv_sge[i].addr =
      (uint64_t)client_metadata_mr[i]->addr; // same as &client_buffer_attr
  client_recv_sge[i].length = client_metadata_mr[i]->length;
  client_recv_sge[i].lkey = client_metadata_mr[i]->lkey;
  /* Now we link this SGE to the work request (WR) */
  client_recv_wr.push_back({});
  bzero(&client_recv_wr[i], sizeof(client_recv_wr[i]));
  client_recv_wr[i].sg_list = &client_recv_sge[i];
  client_recv_wr[i].num_sge = 1; // only one SGE
  bad_client_recv_wr.push_back({});
  ret = ibv_post_recv(client_qp[i] /* which QP */,
                      &client_recv_wr[i] /* receive work request*/,
                      &bad_client_recv_wr[i] /* error WRs */);
  if (ret) {
    rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
    return ret;
  }
  debug("Receive buffer pre-posting is successful \n");
  /* Now we accept the connection. Recall we have not accepted the connection
   * yet because we have to do lots of resource pre-allocation */
  memset(&conn_param, 0, sizeof(conn_param));
  /* this tell how many outstanding requests can we handle */
	conn_param.initiator_depth =
      3; /* For this exercise, we put a small number here */
  /* This tell how many outstanding requests we expect other side to handle */
  conn_param.responder_resources =
      3; /* For this exercise, we put a small number */
  ret = rdma_accept(cm_client_id[i], &conn_param);
  if (ret) {
    rdma_error("Failed to accept the connection, errno: %d \n", -errno);
    return -errno;
  }
  /* We expect an RDMA_CM_EVNET_ESTABLISHED to indicate that the RDMA
   * connection has been established and everything is fine on both, server
   * as well as the client sides.
   */
  debug("Going to wait for : RDMA_CM_EVENT_ESTABLISHED event \n");
  ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_ESTABLISHED,
                              &cm_event);
  if (ret) {
    rdma_error("Failed to get the cm event, errnp: %d \n", -errno);
    return -errno;
  }
  /* We acknowledge the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    rdma_error("Failed to acknowledge the cm event %d\n", -errno);
	  return -errno;
  }
  /* Just FYI: How to extract connection information */
  memcpy(&remote_sockaddr /* where to save */,
         rdma_get_peer_addr(cm_client_id[i]) /* gives you remote sockaddr */,
         sizeof(struct sockaddr_in) /* max size */);
	if (i == 0)
  	printf("A new connection is accepted from %s \n",
         		inet_ntoa(remote_sockaddr.sin_addr));
  
  return ret;
}

/* This function sends server side buffer metadata to the connected client */
static int send_server_metadata_to_client(int i) {
  struct ibv_wc wc;
  int ret = -1;

	// for NO.i connection
	if (i == 0) {
  	server_metadata_attr.push_back({});
  	server_buffer_mr.push_back({});
  	server_metadata_mr.push_back({});
	}
  
  /* Now, we first wait for the client to start the communication by
   * sending the server its metadata info. The server does not use it
   * in our example. We will receive a work completion notification for
   * our pre-posted receive request.
   */
  ret = process_work_completion_events(io_completion_channel[i], &wc, 1);
  if (ret != 1) {
    rdma_error("Failed to receive , ret = %d \n", ret);
    return ret;
  }
  /* if all good, then we should have client's buffer information, lets see */
  //printf("Client %d side buffer information is received...\n", i);
  if (i == 0) {
		show_rdma_buffer_attr(&client_metadata_attr[i]);
  	printf("The clients has requested buffer length of : %lu bytes \n",
  	        client_metadata_attr[i].length);
	}
  /* We need to setup requested memory buffer. This is where the client will
   * do RDMA READs and WRITEs. */

  server_metadata_attr[0].push_back({});
  server_buffer_mr[0].push_back({});
  server_metadata_mr[0].push_back({});

  server_metadata_attr[0][i].length = client_metadata_attr[i].length;
  if (i == 0) {
  	server_buffer.push_back({});
  	server_buffer[0] = calloc(1, server_metadata_attr[0][0].length);
  	if (!server_buffer[0]) {
  	  rdma_error("Server failed to allocate buffer \n");
  	  return -ENOMEM;
  	}
  }
  server_buffer_mr[0][i] = rdma_buffer_register(
      pd[i] /* which protection domain */,
      server_buffer[0],
      server_metadata_attr[0][i].length /* what size to allocate */,
      (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
      IBV_ACCESS_REMOTE_WRITE) /* access permissions */);
  if (!server_buffer_mr[0][i]) {
    rdma_error("Server failed to create a buffer \n");
    /* we assume that it is due to out of memory error */
    return -ENOMEM;
  }
  /* This buffer is used to transmit information about the above
   * buffer to the client. So this contains the metadata about the server
   * buffer. Hence this is called metadata buffer. Since this is already
   * on allocated, we just register it.
   * We need to prepare a send I/O operation that will tell the
   * client the address of the server buffer.
   */
  server_metadata_attr[0][i].address = (uint64_t)server_buffer_mr[0][i]->addr;
  server_metadata_attr[0][i].length = (uint64_t)server_buffer_mr[0][i]->length;
  server_metadata_attr[0][i].stag.local_stag = (uint32_t)server_buffer_mr[0][i]->lkey;
  server_metadata_mr[0][i] = rdma_buffer_register(
      pd[i] /* which protection domain*/,
      &server_metadata_attr[0][i] /* which memory to register */,
      sizeof(server_metadata_attr[0][i]) /* what is the size of memory */,
      IBV_ACCESS_LOCAL_WRITE /* what access permission */);
  if (!server_metadata_mr[0][i]) {
    rdma_error("Server failed to create to hold server metadata \n");
    /* we assume that this is due to out of memory error */
    return -ENOMEM;
  }
  /* We need to transmit this buffer. So we create a send request.
   * A send request consists of multiple SGE elements. In our case, we only
   * have one
   */
  server_send_sge.push_back({});
  server_send_wr.push_back({});
  server_send_sge[i].addr = (uint64_t)&server_metadata_attr[0][i];
  server_send_sge[i].length = sizeof(server_metadata_attr[0][i]);
  server_send_sge[i].lkey = server_metadata_mr[0][i]->lkey;
  /* now we link this sge to the send request */
  bzero(&server_send_wr[i], sizeof(server_send_wr[i]));
  server_send_wr[i].sg_list = &server_send_sge[i];
  server_send_wr[i].num_sge = 1;          // only 1 SGE element in the array
  server_send_wr[i].opcode = IBV_WR_SEND; // This is a send request
  server_send_wr[i].send_flags = IBV_SEND_SIGNALED; // We want to get notification
  /* This is a fast data path operation. Posting an I/O request */
  ret = ibv_post_send(
      client_qp[i] /* which QP */,
      &server_send_wr[i] /* Send request that we prepared before */, &bad_server_send_wr[i] /* In case of error, this will contain failed requests */);
  if (ret) {
    rdma_error("Posting of server metdata failed, errno: %d \n", -errno);
    return -errno;
  }
  /* We check for completion notification */
  ret = process_work_completion_events(io_completion_channel[i], &wc, 1);
  if (ret != 1) {
    rdma_error("Failed to send server metadata, ret = %d \n", ret);
    return ret;
  }
  debug("Local buffer metadata has been sent to the client \n");
  
  return 0;
}

/* This is server side logic. Server passively waits for the client to call
 * rdma_disconnect() and then it will clean up its resources */
static int disconnect_and_cleanup() {
  int ret = -1;

  // disconnect  and clean up each connection
  for (int i = 0; i < connect_num; ++i) {

  	struct rdma_cm_event *cm_event = NULL;
  	/* Now we wait for the client to send us disconnect event */
  	debug("Waiting for cm event: RDMA_CM_EVENT_DISCONNECTED\n");
  	ret = process_rdma_cm_event(cm_event_channel, RDMA_CM_EVENT_DISCONNECTED,
  	                            &cm_event);
  	if (ret) {
  	  rdma_error("Failed to get disconnect event, ret = %d \n", ret);
  	  return ret;
  	}
  	/* We acknowledge the event */
  	ret = rdma_ack_cm_event(cm_event);
  	if (ret) {
  	  rdma_error("Failed to acknowledge the cm event %d\n", -errno);
  	  return -errno;
  	}
  	//printf("A disconnect event is received from the client %d...\n", i);
  	/* We free all the resources */
  	/* Destroy QP */
  	rdma_destroy_qp(cm_client_id[i]);
  	/* Destroy client cm id */
  	ret = rdma_destroy_id(cm_client_id[i]);
  	if (ret) {
  	  rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
  	  // we continue anyways;
  	}
  	/* Destroy CQ */
  	ret = ibv_destroy_cq(cq[i]);
  	if (ret) {
  	  rdma_error("Failed to destroy completion queue cleanly, %d \n", -errno);
  	  // we continue anyways;
  	}
  	/* Destroy completion channel */
  	ret = ibv_destroy_comp_channel(io_completion_channel[i]);
  	if (ret) {
  	  rdma_error("Failed to destroy completion channel cleanly, %d \n", -errno);
  	  // we continue anyways;
  	}
  	/* Destroy memory buffers */
  	for (int j = 0; j < server_buffer.size(); ++j) {
	  	rdma_buffer_deregister(server_buffer_mr[j][i]);
  	  rdma_buffer_deregister(server_metadata_mr[j][i]);
			if (i == connect_num-1)
				free(server_buffer[j]);
  	}
		
  	rdma_buffer_deregister(client_metadata_mr[i]);
  	/* Destroy protection domain */
  	ret = ibv_dealloc_pd(pd[i]);
  	if (ret) {
  	  rdma_error("Failed to destroy client protection domain cleanly, %d \n",
  	             -errno);
  	  // we continue anyways;
  	}

  }

  /* Destroy rdma server id */
  ret = rdma_destroy_id(cm_server_id);
  if (ret) {
    rdma_error("Failed to destroy server id cleanly, %d \n", -errno);
    // we continue anyways;
  }
  rdma_destroy_event_channel(cm_event_channel);

  printf("Server shut-down is complete \n");
  return 0;
}

void usage() {
  printf("Usage:\n");
  printf("rdma_server: [-a <server_addr>] [-p <server_port>] [-n <thread_num>]\n");
  printf("(default port is %d)\n", DEFAULT_RDMA_PORT);
  exit(1);
}

/* Initialize a rdma server with argvs */
int rdma_server_init(int argc, char **argv) {
  int ret, option;

  struct sockaddr_in server_sockaddr;

  bzero(&server_sockaddr, sizeof server_sockaddr);
  server_sockaddr.sin_family = AF_INET; /* standard IP NET address */
  server_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY); /* passed address */
  /* Parse Command Line Arguments, not the most reliable code */
  while ((option = getopt(argc, argv, "a:p:n:")) != -1) {
    switch (option) {
    case 'a':
      /* Remember, this will overwrite the port info */
      ret = get_addr(optarg, (struct sockaddr *)&server_sockaddr);
      tcp_server_addr = server_sockaddr.sin_addr.s_addr;
      if (ret) {
        rdma_error("Invalid IP \n");
        return ret;
      }
      break;
    case 'p':
      /* passed port to listen on */
      server_sockaddr.sin_port = htons(strtol(optarg, NULL, 0));
      break;
		case 'n':
	  	/* multiple threads info */
	  	connect_num = strtol(optarg, NULL, 0);
			if (connect_num != 1)
				connect_num = connect_num*2;
	  	break;
  	default:
      usage();
      break;
    }
  }
  if (!server_sockaddr.sin_port) {
    /* If still zero, that mean no port info provided */
    server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT); /* use default port */
  }
  ret = start_rdma_server(&server_sockaddr);
  if (ret) {
    rdma_error("RDMA server failed to start cleanly, ret = %d \n", ret);
    return ret;
  }

	// for each connection
	for (int i = 0; i < connect_num; ++i) {
		ret = rdma_server_listen(i);
		if (ret) {
			rdma_error("Failed to get a client\n");
			return ret;
		}

  	ret = setup_client_resources(i);
  	if (ret) {
  	  rdma_error("Failed to setup client resources, ret = %d \n", ret);
  	  return ret;
  	}
  	ret = accept_client_connection(i);
  	if (ret) {
  	  rdma_error("Failed to handle client cleanly, ret = %d \n", ret);
  	  return ret;
  	}
  	ret = send_server_metadata_to_client(i);
  	if (ret) {
  	  rdma_error("Failed to send server metadata to the client, ret = %d \n",
  	             ret);
  	  return ret;
  	}
	}

	printf("%d servers are ready\n", max(1, connect_num/2));
  return ret;
}

/* Close the rdma server */
int rdma_server_close() {
  int ret;
  ret = disconnect_and_cleanup();
  if (ret) {
    rdma_error("Failed to clean up resources properly, ret = %d \n", ret);
    return ret;
  }
  return ret;
}