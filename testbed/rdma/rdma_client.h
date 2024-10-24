#ifndef _RDMA_CLIENT_H_
#define _RDMA_CLIENT_H_
/*
 * RDMA client side code for CuckooDuo.
 */

#include "rdma_common.h"
#include <vector>
#include <string>
#include <iostream>

/* Definitions of entry, remote address info and buffers */
/* definition of entry */
#ifndef _ENTRY_H_
#define _ENTRY_H_
#define KEY_LEN 8
#define VAL_LEN 8
#define KV_LEN (KEY_LEN + VAL_LEN)
struct Entry{
    char key[KEY_LEN];
    char val[VAL_LEN];
};
#endif
#define BUF_NUM 16
#ifndef _BUCKET_H_H
#define _BUCKET_H_H
#define N 8
#endif
/* definition of remote address info */
struct RemoteAddr{
    int idx;
    uint64_t offset;

    RemoteAddr& operator=(const RemoteAddr& tmp_ra) {
      idx = tmp_ra.idx;
      offset = tmp_ra.offset;
      return *this;
    }
};
/* buffer for entries */
struct entry_BUF_NUM {
	Entry buf[BUF_NUM];

	entry_BUF_NUM(const entry_BUF_NUM& b) {
		memcpy(buf, b.buf, BUF_NUM*sizeof(Entry));
	}
	entry_BUF_NUM() = default;

	Entry& operator [] (int i) {
		return buf[i];
	}
};
/* buffer for two buckets with N entries */
struct entry_2N {
	Entry buf[2][2*N];

	entry_2N(const entry_2N& b) {
		memcpy(buf, b.buf, 2*2*N*sizeof(Entry));
	}
	entry_2N() = default;

	Entry* operator [] (int i) {
		return buf[i];
	}
};
/* buffer for RemoteAddr */
struct ra_BUF_NUM {
	RemoteAddr buf[BUF_NUM];

	ra_BUF_NUM(const ra_BUF_NUM& r) {
		memcpy(buf, r.buf, BUF_NUM*sizeof(RemoteAddr));
	}
	ra_BUF_NUM() = default;

	RemoteAddr& operator [] (int i) {
		return buf[i];
	}
};
/* buffer for reading of MapEmbed */
struct entry_BUF_ME {
	Entry buf[BUF_NUM][N];

	entry_BUF_ME(const entry_BUF_ME& b) {
		memcpy(buf, b.buf, BUF_NUM*N*sizeof(Entry));
	}
	entry_BUF_ME() = default;

	Entry* operator [] (int i) {
		return buf[i];
	}
};

/* These are basic RDMA resources */
/* These are RDMA connection related resources */
static std::vector<rdma_event_channel *> cm_event_channel;
static std::vector<rdma_cm_id *> cm_client_id;
static std::vector<ibv_pd *> pd;
static std::vector<ibv_comp_channel *> io_completion_channel;
static std::vector<ibv_cq *> client_cq;
static std::vector<ibv_qp_init_attr> qp_init_attr;
static std::vector<ibv_qp *> client_qp;
/* These are memory buffers related resources */
static std::vector<ibv_mr *> client_metadata_mr, client_write_mr[BUF_NUM],
                      		 client_read_mr[BUF_NUM], client_bucket_mr[2];
static std::vector<std::vector<ibv_mr *> > server_metadata_mr;
static std::vector<rdma_buffer_attr> client_metadata_attr;
static std::vector<std::vector<rdma_buffer_attr> > server_metadata_attr;
/* These are exchange infomation related resources */
static std::vector<ibv_send_wr> client_send_wr; 
static std::vector<ibv_send_wr *> bad_client_send_wr;
static std::vector<ibv_recv_wr> server_recv_wr;
static std::vector<ibv_recv_wr *> bad_server_recv_wr;
static std::vector<ibv_sge> client_send_sge, server_recv_sge;
/* These are pointers of Compeletion Queues */
static std::vector<ibv_cq *> cq_ptr;

/* buffers for reading/writing an entry */
static std::vector<entry_BUF_NUM> write_buf;
static std::vector<entry_BUF_NUM> read_buf;
/* buffers for remote address info */
static std::vector<ra_BUF_NUM> ra;
/* buffers for reading two buckets */
static std::vector<entry_2N> bucket_buf;
/* buffers for reading/writing at most BUF_NUM buckets */
static std::vector<entry_BUF_ME> ME_read_buf;
static std::vector<entry_BUF_ME> ME_write_buf;
static std::vector<ibv_mr *> ME_read_mr[BUF_NUM], ME_write_mr[BUF_NUM];

/* the number of RDMA connections */
static int connect_num = 1;
/* the size of each memory block in remote */
static uint64_t rlen = (uint64_t)(1<<30);
/* the number of entries in a block */
static uint64_t cell_num;
/* the number of buckets in a table for CuckooDuo */
static uint64_t bucket_num = (1 << 20) / N / 2;

/* buffers for clean in expansion */
static struct Entry clean_buf[2][5][N];
static struct ibv_mr *clean_mr[2];

/* record tcp server address */
static int tcp_server_addr = 0;

/* A helper to send tcp info */
static inline int send_msg(string msg, int sock) {
	int send_len = send(sock, msg.c_str(), msg.length(), 0);
  if (send_len <= 0)
    cout << "TCP send error" << endl;
  return send_len;
} 

/* Function to set a connection of tcp, return a socket fd */
static inline int set_tcp_client() {
  int ret = -1;
  int sock = socket(AF_INET, SOCK_STREAM, 0);

	sockaddr_in server_addr;
  server_addr.sin_port = htons(53101);
	server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = tcp_server_addr;

  printf("TCP Client is trying to connect...\n");
	if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
		perror("connect failed");
  	return -1;
	}
  printf("TCP Connection: OK\n");

  ret = sock;
  return ret;
}


/* Get five buckets for cleaning table locally */
static inline int get_clean_info(int num = 5) {
  struct ibv_wc wc;
  int ret = -1;

  client_send_sge[connect_num-1].addr = (uint64_t)clean_mr[0]->addr;
  client_send_sge[connect_num-1].length = (uint64_t)clean_mr[0]->length/5*num;
  client_send_sge[connect_num-1].lkey = clean_mr[0]->lkey;
  /* now we link to the send work request */
  bzero(&client_send_wr[connect_num-1], sizeof(client_send_wr[connect_num-1]));
  client_send_wr[connect_num-1].sg_list = &client_send_sge[connect_num-1];
  client_send_wr[connect_num-1].num_sge = 1;
  client_send_wr[connect_num-1].opcode = IBV_WR_RDMA_READ;
  client_send_wr[connect_num-1].send_flags = IBV_SEND_SIGNALED;
  /* we have to tell server side info for RDMA */
  client_send_wr[connect_num-1].wr.rdma.rkey = server_metadata_attr[ra[0][0].idx][connect_num-1].stag.remote_stag;
  client_send_wr[connect_num-1].wr.rdma.remote_addr = server_metadata_attr[ra[0][0].idx][connect_num-1].address+ra[0][0].offset;
  
  /* Now we post it */  
  ret = ibv_post_send(client_qp[connect_num-1], &client_send_wr[connect_num-1], &bad_client_send_wr[connect_num-1]);
  if (ret) {
    rdma_error("Failed to write client clean buffer, errno: %d \n", -errno);
    return -errno;
  }

  ret = my_process_work_completion_events(cq_ptr[connect_num-1], &wc, 1);

  if (ret != 1) {
    rdma_error("We failed to get 1 work completions , ret = %d \n", ret);
    return ret;
  }

  return ret;
}

/* For expand2 */
static inline int mod_clean_info(int num = 5) {
  struct ibv_wc wc[2];
  int ret = -1;

  for (int j = 0; j < 2; ++j) {
    client_send_sge[connect_num-1].addr = (uint64_t)clean_mr[j]->addr;
    client_send_sge[connect_num-1].length = (uint64_t)clean_mr[j]->length/5*num;
    client_send_sge[connect_num-1].lkey = clean_mr[j]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr[connect_num-1], sizeof(client_send_wr[connect_num-1]));
    client_send_wr[connect_num-1].sg_list = &client_send_sge[connect_num-1];
    client_send_wr[connect_num-1].num_sge = 1;
    client_send_wr[connect_num-1].opcode = IBV_WR_RDMA_WRITE;
    client_send_wr[connect_num-1].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[connect_num-1].wr.rdma.rkey = server_metadata_attr[ra[0][j].idx][connect_num-1].stag.remote_stag;
    client_send_wr[connect_num-1].wr.rdma.remote_addr = server_metadata_attr[ra[0][j].idx][connect_num-1].address+ra[0][j].offset;

    //cout << hex << client_send_wr[connect_num-1].wr.rdma.remote_addr << endl;
    //cout << client_send_wr[connect_num-1].wr.rdma.rkey << endl;

    /* Now we post it */  
    ret = ibv_post_send(client_qp[connect_num-1], &client_send_wr[connect_num-1], &bad_client_send_wr[connect_num-1]);
    if (ret) {
      rdma_error("Failed to write client clean buffer, errno: %d \n", -errno);
      return -errno;
    }
  }

  ret = my_process_work_completion_events(cq_ptr[connect_num-1], wc, 2);

  if (ret != 2) {
    rdma_error("We failed to get 2 work completions , ret = %d \n", ret);
    return ret;
  }

  return ret;
}

/* Extend remote memory to n times */
static inline int expand_remote(int n, int copyFlag = 0) {
  int old_sz = server_metadata_mr.size();
  int new_sz;
  if (copyFlag != 3)
    // CuckooDuo and RACE allocate old_sz blocks each time
    new_sz = n*old_sz;
  else
    // Mapembed allocates a new block with new_sz each time
    new_sz = old_sz+1;

  struct ibv_wc wc;
  int ret = -1;

	// set expansion info: n times
  client_metadata_attr[connect_num-1].address = 0;
  client_metadata_attr[connect_num-1].length = n;
  client_metadata_attr[connect_num-1].stag.local_stag = 0;

  client_send_sge[connect_num-1].addr = (uint64_t)client_metadata_mr[connect_num-1]->addr;
  client_send_sge[connect_num-1].length = (uint64_t)client_metadata_mr[connect_num-1]->length;
  client_send_sge[connect_num-1].lkey = client_metadata_mr[connect_num-1]->lkey;
  /* now we link to the send work request */
  bzero(&client_send_wr[connect_num-1], sizeof(client_send_wr[connect_num-1]));
  client_send_wr[connect_num-1].sg_list = &client_send_sge[connect_num-1];
  client_send_wr[connect_num-1].num_sge = 1;
  client_send_wr[connect_num-1].opcode = IBV_WR_SEND;
  client_send_wr[connect_num-1].send_flags = IBV_SEND_SIGNALED;
  /* Now we post it */
  ret = ibv_post_send(client_qp[connect_num-1], &client_send_wr[connect_num-1], &bad_client_send_wr[connect_num-1]);
  if (ret) {
    rdma_error("Failed to send client expansion info, errno: %d \n", -errno);
    return -errno;
  }
  //ret = process_work_completion_events(io_completion_channel[connect_num-1], &wc, 1);
  ret = my_process_work_completion_events(cq_ptr[connect_num-1], &wc, 1);
  if (ret != 1) {
    rdma_error("We failed to get work completion, ret = %d \n", ret);
    return ret;
  }

  // update info for each connection
  for (int i = 0; i < connect_num; ++i) {

  	for (int j = old_sz; j < new_sz; ++j) {
			if (i == 0) {
  	  	server_metadata_attr.push_back({});
  	  	server_metadata_mr.push_back({});
			}

			server_metadata_attr[j].push_back({});
  		server_metadata_mr[j].push_back({});

  	  server_metadata_mr[j][i] = rdma_buffer_register(pd[i], &server_metadata_attr[j][i],
  	                                          sizeof(server_metadata_attr[j][i]),
  	                                          (IBV_ACCESS_LOCAL_WRITE));

  	  server_recv_sge[i].addr = (uint64_t)server_metadata_mr[j][i]->addr;
  	  server_recv_sge[i].length = (uint64_t)server_metadata_mr[j][i]->length;
  	  server_recv_sge[i].lkey = (uint32_t)server_metadata_mr[j][i]->lkey;
  	  /* now we link it to the request */
  	  bzero(&server_recv_wr[i], sizeof(server_recv_wr[i]));
  	  server_recv_wr[i].sg_list = &server_recv_sge[i];
  	  server_recv_wr[i].num_sge = 1;
  	  ret = ibv_post_recv(client_qp[i] /* which QP */,
  	                      &server_recv_wr[i] /* receive work request*/,
  	                      &bad_server_recv_wr[i] /* error WRs */);
  	  if (ret) {
  	    rdma_error("Failed to pre-post the server metadata buffer, errno: %d \n", ret);
  	    return ret;
  	  }

  	  //ret = process_work_completion_events(io_completion_channel[i], &wc, 1);
  	  ret = my_process_work_completion_events(cq_ptr[i], &wc, 1);
  	  if (ret != 1) {
  	    rdma_error("We failed to get  work completion , ret = %d \n", ret);
  	    return ret;
  	  }

			/*if (i == connect_num-1) {
  	  	printf("tid: %d, MR[%d]:\n", i, j);
  	  	show_rdma_buffer_attr(&server_metadata_attr[j][i]);
			}*/
  	}
  }
	  
  return 0;
}

/* Calculate the location in remote corresponding to lacal one */
static inline RemoteAddr get_offset_table(int table_i, int bucket_i, int cell_i, int type=0) {
  RemoteAddr ra;

  if (type == 0) {
    // for race and tea
    ra.idx = bucket_i/(cell_num/N);

    ra.offset = 0;
    ra.offset += (bucket_i%(cell_num/N))*N*KV_LEN;
    ra.offset += cell_i*(KV_LEN);
  }
  else if (type == 1) {
    // for cuckooduo
  	ra.idx = bucket_i/(cell_num/2/N);

    ra.offset = table_i *(cell_num/2)*(KV_LEN);
    ra.offset += (bucket_i%(cell_num/2/N))*N*(KV_LEN);
    ra.offset += cell_i*(KV_LEN);
  }
  else if (type == 2) {
    // for mapembed
    ra.idx = server_metadata_mr.size()-1;

    ra.offset = 0;
    ra.offset += (bucket_i)*N*KV_LEN;
    ra.offset += cell_i*(KV_LEN);
  }

  ra.offset %= (rlen);
  return ra;
}

/* Get sz buckets with the tid connection sequentially with doorbell batching optimization */
static inline int get_ME(int sz, int tid) {
  struct ibv_wc wc[BUF_NUM];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
    client_send_sge[tid].addr = (uint64_t)ME_read_mr[j][tid]->addr;
    client_send_sge[tid].length = (uint64_t)ME_read_mr[j][tid]->length;
    client_send_sge[tid].lkey = ME_read_mr[j][tid]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
    client_send_wr[tid].sg_list = &client_send_sge[tid];
    client_send_wr[tid].num_sge = 1;
    client_send_wr[tid].opcode = IBV_WR_RDMA_READ;
    client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
    client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
    /* Now we post it */
    ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);

    if (ret) {
      rdma_error("Failed to read a ME bucket buffer from the server, errno: %d \n",-errno);
      return -errno;
    }
  }

  /* at this point we are expecting sz work completion for the write */
  //ret = process_work_completion_events(io_completion_channel, wc, sz);
  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get %d work completions , ret = %d \n", sz, ret);
    return ret;
  }

  return 0;
}

/* Modify sz buckets with the tid connection sequentially with doorbell batching optimization */
static inline int mod_ME(int sz, int tid) {
  struct ibv_wc wc[BUF_NUM];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
    /* now we fill up SGE */
    client_send_sge[tid].addr = (uint64_t)ME_write_mr[j][tid]->addr;
    client_send_sge[tid].length = (uint64_t)ME_write_mr[j][tid]->length;
    client_send_sge[tid].lkey = ME_write_mr[j][tid]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
    client_send_wr[tid].sg_list = &client_send_sge[tid];
    client_send_wr[tid].num_sge = 1;
    client_send_wr[tid].opcode = IBV_WR_RDMA_WRITE;
    client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
    client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
    /* Now we post it */  
    ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);

    if (ret) {
      rdma_error("Failed to write a ME bucket buffer to server, errno: %d \n", -errno);
      return -errno;
    }
  }

  /* at this point we are expecting sz work completion for the write */
  //ret = process_work_completion_events(io_completion_channel, wc, sz);
  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get %d work completions , ret = %d \n", sz, ret);
    return ret;
  }

  return 0;
}

/* Get sz*b_num buckets with the tid connection sequentially with doorbell batching optimization */
static inline int get_bucket(int b_num, int sz, int tid) {
  struct ibv_wc wc[2];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
    client_send_sge[tid].addr = (uint64_t)client_bucket_mr[j][tid]->addr;

    if (b_num == 2)
			// each buffer with two buckets
      client_send_sge[tid].length = (uint64_t)client_bucket_mr[j][tid]->length;
    else
			// each buffer with one bucket
      client_send_sge[tid].length = (uint64_t)client_bucket_mr[j][tid]->length/2;

    client_send_sge[tid].lkey = client_bucket_mr[j][tid]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
    client_send_wr[tid].sg_list = &client_send_sge[tid];
    client_send_wr[tid].num_sge = 1;
    client_send_wr[tid].opcode = IBV_WR_RDMA_READ;
    client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
    client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
    /* Now we post it */  
    ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);

    if (ret) {
      rdma_error("Failed to write read a bucket buffer from server, errno: %d \n", -errno);
      return -errno;
    }
  }

  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get %d work completions , ret = %d \n", sz, ret);
    return ret;
  }

  return ret;
}

/* Modify sz entries with the tid connection sequentially with doorbell batching optimization */
static inline int mod_remote(int sz, int tid) {
  struct ibv_wc wc[BUF_NUM];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
    /* now we fill up SGE */
    client_send_sge[tid].addr = (uint64_t)client_write_mr[j][tid]->addr;
    client_send_sge[tid].length = (uint64_t)client_write_mr[j][tid]->length;
    client_send_sge[tid].lkey = client_write_mr[j][tid]->lkey;
		
    /* now we link to the send work request */
    bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
    client_send_wr[tid].sg_list = &client_send_sge[tid];
    client_send_wr[tid].num_sge = 1;
    client_send_wr[tid].opcode = IBV_WR_RDMA_WRITE;
    client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
    client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
    /* Now we post it */  
    ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);

    if (ret) {
      rdma_error("Failed to write an entry to the remote, errno: %d \n", -errno);
      return -errno;
    }
  }

  /* at this point we are expecting sz work completion for the write */
  //ret = process_work_completion_events(io_completion_channel[tid], wc, sz);
  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get %d work completions , ret = %d \n", sz, ret);
    return ret;
  }

  return 0;
}

/* Get sz entries with the tid connection sequentially with doorbell batching optimization */
static inline int get_remote(int sz, int tid) {
  struct ibv_wc wc[BUF_NUM];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
    client_send_sge[tid].addr = (uint64_t)client_read_mr[j][tid]->addr;
    client_send_sge[tid].length = (uint64_t)client_read_mr[j][tid]->length;
    client_send_sge[tid].lkey = client_read_mr[j][tid]->lkey;
    /* now we link to the send work request */
    bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
    client_send_wr[tid].sg_list = &client_send_sge[tid];
    client_send_wr[tid].num_sge = 1;
    client_send_wr[tid].opcode = IBV_WR_RDMA_READ;
    client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
    /* we have to tell server side info for RDMA */
    client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
    client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
    /* Now we post it */
    ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);

    if (ret) {
      rdma_error("Failed to read an entry from the remote, errno: %d \n", -errno);
      return -errno;
    }
  }

  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get %d work completions , ret = %d \n", sz, ret);
    return ret;
  }

  return 0;
}

/* This function prepares client side connection resources for an RDMA
 * connection */
static int client_prepare_connection(struct sockaddr_in *s_addr, int i) {
  struct rdma_cm_event * cm_event = NULL;
  int ret = -1;

	// for NO.i connetcion
  /*  Open a channel used to report asynchronous communication event */
	cm_event_channel.push_back({});
  cm_event_channel[i] = rdma_create_event_channel();
  if (!cm_event_channel[i]) {
    rdma_error("Creating cm event channel failed, errno: %d \n", -errno);
    return -errno;
  }
  debug("RDMA CM event channel is created at : %p \n", cm_event_channel);
  /* rdma_cm_id is the connection identifier (like socket) which is used
   * to define an RDMA connection.
   */
  cm_client_id.push_back({});
  ret = rdma_create_id(cm_event_channel[i], &cm_client_id[i], NULL, RDMA_PS_TCP);
  if (ret) {
    rdma_error("Creating cm id failed with errno: %d \n", -errno);
    return -errno;
  }
  /* Resolve destination and optional source addresses from IP addresses  to
   * an RDMA address.  If successful, the specified rdma_cm_id will be bound
   * to a local device. */
  ret = rdma_resolve_addr(cm_client_id[i], NULL, (struct sockaddr *)s_addr, 2000);
  if (ret) {
    rdma_error("Failed to resolve address, errno: %d \n", -errno);
    return -errno;
  }
  debug("waiting for cm event: RDMA_CM_EVENT_ADDR_RESOLVED\n");
  ret = process_rdma_cm_event(cm_event_channel[i], RDMA_CM_EVENT_ADDR_RESOLVED,
                              &cm_event);
  if (ret) {
    rdma_error("Failed to receive a valid event, ret = %d \n", ret);
    return ret;
  }
  /* we ack the event */
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    rdma_error("Failed to acknowledge the CM event, errno: %d\n", -errno);
    return -errno;
  }
  debug("RDMA address is resolved \n");

	/* Resolves an RDMA route to the destination address in order to
	* establish a connection */
	ret = rdma_resolve_route(cm_client_id[i], 2000);
	if (ret) {
	  rdma_error("Failed to resolve route, erno: %d \n", -errno);
	  return -errno;
	}
	debug("waiting for cm event: RDMA_CM_EVENT_ROUTE_RESOLVED\n");
	ret = process_rdma_cm_event(cm_event_channel[i], RDMA_CM_EVENT_ROUTE_RESOLVED,
	                            &cm_event);
	if (ret) {
	  rdma_error("Failed to receive a valid event, ret = %d \n", ret);
	  return ret;
	}
	/* we ack the event */
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
	  rdma_error("Failed to acknowledge the CM event, errno: %d \n", -errno);
	  return -errno;
	}
	//printf("Trying to connect to server at : %s port: %d \n",
	//       inet_ntoa(s_addr->sin_addr), ntohs(s_addr->sin_port));
	/* Protection Domain (PD) is similar to a "process abstraction"
	 * in the operating system. All resources are tied to a particular PD.
	 * And accessing recourses across PD will result in a protection fault.
	 */
	pd.push_back({});
	pd[i] = ibv_alloc_pd(cm_client_id[i]->verbs);
	if (!pd[i]) {
	  rdma_error("Failed to alloc pd, errno: %d \n", -errno);
	  return -errno;
	}
	debug("pd allocated at %p \n", pd[i]);
	/* Now we need a completion channel, were the I/O completion
	 * notifications are sent. Remember, this is different from connection
	 * management (CM) event notifications.
	 * A completion channel is also tied to an RDMA device, hence we will
	 * use cm_client_id->verbs.
	 */
	io_completion_channel.push_back({});
 	io_completion_channel[i] = ibv_create_comp_channel(cm_client_id[i]->verbs);
	if (!io_completion_channel[i]) {
  rdma_error("Failed to create IO completion event channel, errno: %d\n",
             -errno);
	  return -errno;
	}
	debug("completion event channel created at : %p \n", io_completion_channel[i]);
	/* Now we create a completion queue (CQ) where actual I/O
	 * completion metadata is placed. The metadata is packed into a structure
   * called struct ibv_wc (wc = work completion). ibv_wc has detailed
   * information about the work completion. An I/O request in RDMA world
   * is called "work" ;)
   */
  client_cq.push_back({});
  client_cq[i] = ibv_create_cq(
      cm_client_id[i]->verbs /* which device*/, CQ_CAPACITY /* maximum capacity*/,
      NULL /* user context, not used here */,
      io_completion_channel[i] /* which IO completion channel */,
      0 /* signaling vector, not used here*/);
  if (!client_cq[i]) {
    rdma_error("Failed to create CQ, errno: %d \n", -errno);
    return -errno;
  }
  debug("CQ created at %p with %d elements \n", client_cq[i], client_cq[i]->cqe);
  ret = ibv_req_notify_cq(client_cq[i], 0);
  if (ret) {
    rdma_error("Failed to request notifications, errno: %d\n", -errno);
    return -errno;
  }
  /* Now the last step, set up the queue pair (send, recv) queues and their
   * capacity. The capacity here is define statically but this can be probed
   * from the device. We just use a small number as defined in rdma_common.h */
  qp_init_attr.push_back({});
  bzero(&qp_init_attr[i], sizeof(qp_init_attr[i]));
  qp_init_attr[i].cap.max_recv_sge = MAX_SGE; /* Maximum SGE per receive posting */
  qp_init_attr[i].cap.max_recv_wr = MAX_WR; /* Maximum receive posting capacity */
  qp_init_attr[i].cap.max_send_sge = MAX_SGE; /* Maximum SGE per send posting */
  qp_init_attr[i].cap.max_send_wr = MAX_WR;   /* Maximum send posting capacity */
	qp_init_attr[i].qp_type = IBV_QPT_RC; /* QP type, RC = Reliable connection */
  /* We use same completion queue, but one can use different queues */
  qp_init_attr[i].recv_cq =
      client_cq[i]; /* Where should I notify for receive completion operations */
  qp_init_attr[i].send_cq =
      client_cq[i]; /* Where should I notify for send completion operations */
  /*Lets create a QP */
  ret = rdma_create_qp(cm_client_id[i] /* which connection id */,
                       pd[i] /* which protection domain*/,
                       &qp_init_attr[i] /* Initial attributes */);
  if (ret) {
    rdma_error("Failed to create QP, errno: %d \n", -errno);
    return -errno;
  }
  client_qp.push_back({});
  client_qp[i] = cm_client_id[i]->qp;
  debug("QP created at %p \n", client_qp[i]);

  return 0;
}

/* Pre-posts a receive buffer before calling rdma_connect () */
static int client_pre_post_recv_buffer(int i) {
  int ret = -1;

	// for NO.i connetcion
	if (i == 0) {
  	server_metadata_attr.push_back({});
  	server_metadata_mr.push_back({});
	}
  server_metadata_attr[0].push_back({});
  server_metadata_mr[0].push_back({});

  server_metadata_mr[0][i] = rdma_buffer_register(pd[i], &server_metadata_attr[0][i],
                                            sizeof(server_metadata_attr[0][i]),
                                            (IBV_ACCESS_LOCAL_WRITE));
  if (!server_metadata_mr[0][i]) {
    rdma_error("Failed to setup the server metadata mr , -ENOMEM\n");
    return -ENOMEM;
  }
  server_recv_sge.push_back({});
  server_recv_wr.push_back({});
  server_recv_sge[i].addr = (uint64_t)server_metadata_mr[0][i]->addr;
  server_recv_sge[i].length = (uint64_t)server_metadata_mr[0][i]->length;
  server_recv_sge[i].lkey = (uint32_t)server_metadata_mr[0][i]->lkey;
  /* now we link it to the request */
  bzero(&server_recv_wr[i], sizeof(server_recv_wr[i]));
  server_recv_wr[i].sg_list = &server_recv_sge[i];
  server_recv_wr[i].num_sge = 1;
  bad_server_recv_wr.push_back({});
  ret = ibv_post_recv(client_qp[i] /* which QP */,
                      &server_recv_wr[i] /* receive work request*/,
                      &bad_server_recv_wr[i] /* error WRs */);
  if (ret) {
    rdma_error("Failed to pre-post the receive buffer, errno: %d \n", ret);
    return ret;
  }
  debug("Receive buffer pre-posting is successful \n");

  return 0;
}

/* Connects to the RDMA server */
static int client_connect_to_server(int i) {
  int ret = -1;

	// for NO.i connetcion
  struct rdma_conn_param conn_param;
  struct rdma_cm_event *cm_event = NULL;

  bzero(&conn_param, sizeof(conn_param));
  conn_param.initiator_depth = 3;
	conn_param.responder_resources = 3;
	conn_param.retry_count = 3; // if fail, then how many times to retry
	ret = rdma_connect(cm_client_id[i], &conn_param);
	if (ret) {
 	  rdma_error("Failed to connect to remote host , errno: %d\n", -errno);
  	return -errno;
	}
 	debug("waiting for cm event: RDMA_CM_EVENT_ESTABLISHED\n");
 	ret = process_rdma_cm_event(cm_event_channel[i], RDMA_CM_EVENT_ESTABLISHED,
	                            &cm_event);
	if (ret) {
    rdma_error("Failed to get cm event, ret = %d \n", ret);
    return ret;
	}
	ret = rdma_ack_cm_event(cm_event);
	if (ret) {
 	  rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
 	  return -errno;
 	}
 	//printf("The client %d is connected successfully \n", i);

  /*printf("src port: %d\n", ntohs(rdma_get_src_port(cm_client_id[i])));
	printf("dst port: %d\n", ntohs(rdma_get_dst_port(cm_client_id[i])));

  struct ibv_qp_attr qp_attr;
  ibv_query_qp(client_qp[i], &qp_attr, 0, &qp_init_attr[i]);

  printf("src queue pair: %x\n", client_qp[i]->qp_num);
  printf("dst queue pair: %x\n", qp_attr.dest_qp_num);
  printf("recv queue psn: %d\n", qp_attr.rq_psn+1);
	printf("send queue psn: %d\n", qp_attr.sq_psn+1);

  printf("flow_label: %d\n", qp_attr.ah_attr.grh.flow_label);
  printf("udp_sport: %d\n",
	       ibv_flow_label_to_udp_sport(qp_attr.ah_attr.grh.flow_label));*/

  return 0;
}

/* Exchange buffer metadata with the server. The client sends its, and then
 * receives from the server. The client-side metadata on the server is _not_
 * used because this program is client driven. But it shown here how to do it
 * for the illustration purposes
 */
static int client_xchange_metadata_with_server(int i) {
  // we have cell_num entries each size is KV_LEN
  rlen = cell_num*(KV_LEN);

  struct ibv_wc wc[2];
  int ret = -1;

	// for NO.i conneciton
	if (i == connect_num-1) {
    for (int j = 0; j < 2; ++j) {
		  clean_mr[j] =
      	  rdma_buffer_register(pd[i], clean_buf[j],
        	                     sizeof(struct Entry)*N*5,
          	                   (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
            	                 IBV_ACCESS_REMOTE_WRITE));
  	  if (!clean_mr[j]) {
      	rdma_error("Failed to register the clean buffer, ret = %d \n", ret);
      	return ret;
  	  }
    }
	}

	// for buckets buffer of MapEmbed
	ME_write_buf.push_back({});
	ME_read_buf.push_back({});

  for (int j = 0; j < BUF_NUM; ++j) {
		ME_write_mr[j].push_back({});
	  ME_write_mr[j][i] =
 	      rdma_buffer_register(pd[i], ME_write_buf[i][j],
 	                           sizeof(struct Entry)*N,
	  	                       (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
  	                         IBV_ACCESS_REMOTE_WRITE));
  	if (!ME_write_mr[j][i]) {
  	  rdma_error("Failed to register the ME_write buffer, ret = %d \n", ret);
  	  return ret;
  	}
		ME_read_mr[j].push_back({});
  	ME_read_mr[j][i] =
  	    rdma_buffer_register(pd[i], ME_read_buf[i][j],
  	                         sizeof(struct Entry)*N,
  	                         (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
  	                         IBV_ACCESS_REMOTE_WRITE));
  	if (!ME_read_mr[j][i]) {
  	  rdma_error("We failed to create the ME_read buffer, -ENOMEM\n");
  	  return -ENOMEM;
  	}
  }

  // for buckets buffer of other 3 algorithms
  bucket_buf.push_back({});

	for (int j = 0; j < 2; ++j) {
		client_bucket_mr[j].push_back({});
  	client_bucket_mr[j][i] = 
  	    rdma_buffer_register(pd[i], bucket_buf[i][j],
  	                         sizeof(struct Entry)*N*2,
  	                         (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
  	                         IBV_ACCESS_REMOTE_WRITE));
  	if (!client_bucket_mr[j][i]) {
  	  rdma_error("Failed to register the bucket buffer, ret = %d \n", ret);
  	  return ret;
  	}
	}
  
	// for read/write buffer for entries
	write_buf.push_back({});
  read_buf.push_back({});

	for (int j = 0; j < BUF_NUM; ++j) {
		client_write_mr[j].push_back({});
    client_write_mr[j][i] =
        rdma_buffer_register(pd[i], &write_buf[i][j],
                             sizeof(struct Entry),
                             (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE));
    if (!client_write_mr[j][i]) {
      rdma_error("Failed to register the write buffer, ret = %d \n", ret);
      return ret;
    }
		client_read_mr[j].push_back({});
    client_read_mr[j][i] =
        rdma_buffer_register(pd[i], &read_buf[i][j],
                             sizeof(struct Entry),
                             (IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                             IBV_ACCESS_REMOTE_WRITE));
    if (!client_read_mr[j][i]) {
      rdma_error("We failed to create the read buffer, -ENOMEM\n");
      return -ENOMEM;
    }
	}

  client_metadata_attr.push_back({});
  client_metadata_mr.push_back({});
  /* we prepare metadata for the first buffer */
  client_metadata_attr[i].address = 0;
  client_metadata_attr[i].length = rlen;
  client_metadata_attr[i].stag.local_stag = 0;
  /* now we register the metadata memory */
  client_metadata_mr[i] = rdma_buffer_register(pd[i], &client_metadata_attr[i],
                                            sizeof(client_metadata_attr[i]),
                                            IBV_ACCESS_LOCAL_WRITE);
  if (!client_metadata_mr[i]) {
    rdma_error("Failed to register the client metadata buffer, ret = %d \n",
               ret);
    return ret;
  }

	client_send_sge.push_back({});
  client_send_wr.push_back({});
  /* now we fill up SGE */
  client_send_sge[i].addr = (uint64_t)client_metadata_mr[i]->addr;
  client_send_sge[i].length = (uint64_t)client_metadata_mr[i]->length;
  client_send_sge[i].lkey = client_metadata_mr[i]->lkey;
  /* now we link to the send work request */
  bzero(&client_send_wr[i], sizeof(client_send_wr[i]));
  client_send_wr[i].sg_list = &client_send_sge[i];
  client_send_wr[i].num_sge = 1;
  client_send_wr[i].opcode = IBV_WR_SEND;
  client_send_wr[i].send_flags = IBV_SEND_SIGNALED;
	bad_client_send_wr.push_back({});
  /* Now we post it */
  ret = ibv_post_send(client_qp[i], &client_send_wr[i], &bad_client_send_wr[i]);
  if (ret) {
    rdma_error("Failed to send client metadata, errno: %d \n", -errno);
    return -errno;
	}
  /* at this point we are expecting 2 work completion. One for our
   * send and one for recv that we will get from the server for
   * its buffer information */
  ret = process_work_completion_events(io_completion_channel[i], wc, 2);
  if (ret != 2) {
    rdma_error("We failed to get 2 work completions , ret = %d \n", ret);
    return ret;
  }
  debug("Server sent us its buffer location and credentials, showing \n");

	if (i == 0) {
  	printf("MR[0]:\n");
  	show_rdma_buffer_attr(&server_metadata_attr[0][i]);
	}

  return 0;
}

/* This function does :
 * 1) Prepare memory buffers for RDMA operations
 * 1) RDMA write from src -> remote buffer
 * 2) RDMA read from remote bufer -> dst
 */
static inline int client_remote_memory_ops(int sz, int tid) {
  struct ibv_wc wc[BUF_NUM];
  int ret = -1;

  for (int j = 0; j < sz; ++j) {
  	/* now we fill up SGE */
  	client_send_sge[tid].addr = (uint64_t)client_write_mr[j][tid]->addr;
  	client_send_sge[tid].length = (uint64_t)client_write_mr[j][tid]->length;
  	client_send_sge[tid].lkey = client_write_mr[j][tid]->lkey;
  	/* now we link to the send work request */
  	bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
  	client_send_wr[tid].sg_list = &client_send_sge[tid];
  	client_send_wr[tid].num_sge = 1;
  	client_send_wr[tid].opcode = IBV_WR_RDMA_WRITE;
  	client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
  	/* we have to tell server side info for RDMA */
  	client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
  	client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
  	/* Now we post it */
  	ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);
  	if (ret) {
  	  rdma_error("Failed to write client src buffer, errno: %d \n", -errno);
  	  return -errno;
  	}
  	}
  	/* at this point we are expecting sz work completion for the write */
  	//ret = process_work_completion_events(io_completion_channel[tid], wc, sz);
  	ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);
  	if (ret != sz) {
  	  rdma_error("We failed to get 1 work completions , ret = %d \n", ret);
  	  return ret;
  	}
  	debug("Client side WRITE is complete \n");

  	/* Now we prepare a READ using same variables but for destination */
  	for (int j = 0; j < sz; ++j) {
  	client_send_sge[tid].addr = (uint64_t)client_read_mr[j][tid]->addr;
  	client_send_sge[tid].length = (uint64_t)client_read_mr[j][tid]->length;
  	client_send_sge[tid].lkey = client_read_mr[j][tid]->lkey;
  	/* now we link to the send work request */
  	bzero(&client_send_wr[tid], sizeof(client_send_wr[tid]));
  	client_send_wr[tid].sg_list = &client_send_sge[tid];
  	client_send_wr[tid].num_sge = 1;
  	client_send_wr[tid].opcode = IBV_WR_RDMA_READ;
  	client_send_wr[tid].send_flags = IBV_SEND_SIGNALED;
  	/* we have to tell server side info for RDMA */
  	client_send_wr[tid].wr.rdma.rkey = server_metadata_attr[ra[tid][j].idx][tid].stag.remote_stag;
  	client_send_wr[tid].wr.rdma.remote_addr = server_metadata_attr[ra[tid][j].idx][tid].address+ra[tid][j].offset;
  	/* Now we post it */
  	ret = ibv_post_send(client_qp[tid], &client_send_wr[tid], &bad_client_send_wr[tid]);
  	if (ret) {
  	  rdma_error("Failed to read client dst buffer from the master, errno: %d \n",
  	             -errno);
  	  return -errno;
  	}
  }
  /* at this point we are expecting sz work completion for the write */
  //ret = process_work_completion_events(io_completion_channel[tid], wc, sz);
  ret = my_process_work_completion_events(cq_ptr[tid], wc, sz);

  if (ret != sz) {
    rdma_error("We failed to get 1 work completions , ret = %d \n", ret);
    return ret;
  }
  debug("Client side READ is complete \n");
  
  return 0;
}

/* This function disconnects the RDMA connection from the server and cleans up
 * all the resources.
 */
static int client_disconnect_and_clean() {
  int ret = -1;

	// destory resources for each connection
  for (int i = 0; i < connect_num; ++i) {
  	struct rdma_cm_event *cm_event = NULL;
  	/* active disconnect from the client side */
  	ret = rdma_disconnect(cm_client_id[i]);
  	if (ret) {
  	  rdma_error("Failed to disconnect, errno: %d \n", -errno);
  	  // continuing anyways
  	}
  	ret = process_rdma_cm_event(cm_event_channel[i], RDMA_CM_EVENT_DISCONNECTED,
  	                            &cm_event);
  	if (ret) {
  	  rdma_error("Failed to get RDMA_CM_EVENT_DISCONNECTED event, ret = %d\n",
  	             ret);
  	  // continuing anyways
  	}
  	ret = rdma_ack_cm_event(cm_event);
  	if (ret) {
  	  rdma_error("Failed to acknowledge cm event, errno: %d\n", -errno);
  	  // continuing anyways
  	}
  	/* Destroy QP */
  	rdma_destroy_qp(cm_client_id[i]);
  	/* Destroy client cm id */
  	ret = rdma_destroy_id(cm_client_id[i]);
  	if (ret) {
  	  rdma_error("Failed to destroy client id cleanly, %d \n", -errno);
  	  // we continue anyways;
  	}
  	/* Destroy CQ */
  	ret = ibv_destroy_cq(client_cq[i]);
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
  	for(int j = 0; j < server_metadata_mr.size(); ++j)
  	  rdma_buffer_deregister(server_metadata_mr[j][i]);
  	rdma_buffer_deregister(client_metadata_mr[i]);
  	for (int j = 0; j < BUF_NUM; ++j) {
  		rdma_buffer_deregister(client_write_mr[j][i]);
  		rdma_buffer_deregister(client_read_mr[j][i]);
  	}
  	/* Destroy protection domain */
  	ret = ibv_dealloc_pd(pd[i]);
  	if (ret) {
  	  /*rdma_error("Failed to destroy client protection domain cleanly, %d \n",
  	             -errno);*/
  	  // we continue anyways;
  	}
  	rdma_destroy_event_channel(cm_event_channel[i]);
		//printf("Client %d is over\n", i);
		//sleep(1);
  }

  printf("Client resource clean up is complete \n");
  return 0;
}

static void usage() {
  printf("Usage:\n");
  printf("rdma_client: [-a <server_addr>] [-p <server_port>]"
         "(required)\n");
  printf("(default IP is 127.0.0.1 and port is %d)\n", DEFAULT_RDMA_PORT);
  exit(1);
}

/* Initialize a rdma client with argvs and cell_number */
int rdma_client_init(int argc, char **argv, uint64_t cell_number) {
  cell_num = cell_number;
  bucket_num = cell_num/N/2;

  struct sockaddr_in server_sockaddr;
  int ret, option;
  bzero(&server_sockaddr, sizeof server_sockaddr);
  server_sockaddr.sin_family = AF_INET;
  server_sockaddr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

  /* Parse Command Line Arguments */
  while ((option = getopt(argc, argv, "a:p:n:")) != -1) {
    switch (option) {
    case 'a':
      /* remember, this overwrites the port info */
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

	/* Create a rdma client with connect_num connection to rdma server */
  if (!server_sockaddr.sin_port) {
    /* no port provided, use the default port */
    server_sockaddr.sin_port = htons(DEFAULT_RDMA_PORT);
  }
	/* Initialize each connection */
	for (int i = 0; i < connect_num; ++i) {
  	ret = client_prepare_connection(&server_sockaddr, i);
  	if (ret) {
  	  rdma_error("Failed to setup client connection , ret = %d \n", ret);
  	  return ret;
  	}
  	ret = client_pre_post_recv_buffer(i);
  	if (ret) {
  	  rdma_error("Failed to setup client connection , ret = %d \n", ret);
  	  return ret;
  	}
  	ret = client_connect_to_server(i);
  	if (ret) {
  	  rdma_error("Failed to setup client connection , ret = %d \n", ret);
  	  return ret;
  	}
  	ret = client_xchange_metadata_with_server(i);
  	if (ret) {
  	  rdma_error("Failed to setup client connection , ret = %d \n", ret);
  	  return ret;
  	}

		ra.push_back({});

  	void *context = NULL;
  	cq_ptr.push_back({});
  	ret = ibv_get_cq_event(io_completion_channel[i], /* IO channel where we are expecting the notification */ 
			       &cq_ptr[i], /* which CQ has an activity. This should be the same as CQ we created before */ 
			       &context); /* Associated CQ user context, which we did set */
  	     if (ret) {
		       rdma_error("Failed to get next CQ event due to %d \n", -errno);
		       return -errno;
  	     }
  	ret = ibv_req_notify_cq(cq_ptr[i], 0);
  	     if (ret){
		       rdma_error("Failed to request further notifications %d \n", -errno);
		       return -errno;
  	     }
		//sleep(1);
  }
	printf("%d clients are ready\n", max(1, connect_num/2));

  return ret;
}

/* Close the rdma client */
static int rdma_client_close() {
  printf("The client has sent a disconnect request to the server...\n");
  int ret;

  for (int i = 0; i < connect_num; ++i) {
	/* Similar to connection management events, we need to acknowledge CQ events */
	     ibv_ack_cq_events(cq_ptr[i], 
			       1 /* we received one event notification. This is not 
			       number of WC elements */);
  }

  ret = client_disconnect_and_clean();
  if (ret) {
    rdma_error("Failed to cleanly disconnect and clean up resources \n");
  }
  return ret;
}

#endif
