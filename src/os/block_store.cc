#include <os/block_store.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <iostream>
#include <string.h>

namespace morph {

BlockStore::BlockStore():
    BlockStore(BlockStoreOptions()) 
{}

BlockStore::BlockStore(BlockStoreOptions o):
    opts(o),
    running(true),
    num_in_progress(0),
    ioctx(0) {
  int oflag;

  oflag = O_CREAT | O_RDWR | O_DIRECT | O_SYNC;
  if (!opts.recover) {
    oflag |= O_TRUNC;
  }

  fd = open(opts.STORE_FILE.c_str(), oflag);
  if (fd < 0) {
    perror("failed to open file to store buffers");
    exit(EXIT_FAILURE);
  }

  if (fallocate(fd, 0, 0, opts.block_size * opts.TOTAL_BLOCKS) < 0) {
    perror("failed to fallocate");
    exit(EXIT_FAILURE);
  }

  int x;
  if ((x = io_setup(opts.max_num_event, &ioctx)) < 0) {
    std::cerr << "FAILED " << x << std::endl;
    perror("failed to do io_setup");
    exit(EXIT_FAILURE);
  }

  bitmap = std::make_unique<Bitmap>(opts.TOTAL_BLOCKS, opts.ALLOCATE_RETRY);

  submit_thread = std::make_unique<std::thread>(&BlockStore::submit_routine, this);

  reap_thread = std::make_unique<std::thread>(&BlockStore::reap_routine, this);
}

BlockStore::~BlockStore() {
  if (running) {
    stop();
  }

  if (close(fd) < 0) {
    perror("failed to close block store file");
    exit(EXIT_FAILURE);
  }

  if (io_destroy(ioctx) < 0) {
    perror("failed to io_destory");
    exit(EXIT_FAILURE);
  }
}

// TODO: should these belong to the allocator class?
lbn_t BlockStore::allocate_blocks(uint32_t count) {
  return bitmap->allocate_blocks(count);
}

// TODO: same as above
void BlockStore::free_blocks(lbn_t start_block, uint32_t count) {
  bitmap->free_blocks(start_block, count);
}

void BlockStore::push_request(std::shared_ptr<IoRequest> request) {
  io_requests.push(request);
}

void BlockStore::submit_write(const std::shared_ptr<IoRequest> &request, struct iocb *iocb) {
  uint32_t iovcnt = request->buffers.size();
  uint32_t offset = request->buffers.front()->lbn * opts.block_size;
  struct iovec iov[iovcnt];
  uint32_t i;
  int ret;

  i = 0;
  for (const auto &buffer: request->buffers) {
    if (!flag_marked(buffer, B_DIRTY)) {
      //fprintf(stderr, "[bs] submit_write: buffer %d is not dirty. request first lbn(%d), total(%d)\n", 
      //  buffer->lbn, request->buffers.front()->lbn, request->buffers.size());
    }
    assert(flag_marked(buffer, B_DIRTY));

    iov[i].iov_base = buffer->buf;
    iov[i].iov_len = opts.block_size;
    ++i;

    //fprintf(stderr, "[bs] submit_write: set buffer %d into iovec first lbn(%d)\n", 
    //    buffer->lbn, request->buffers.front()->lbn);
  }
  
  io_prep_pwritev(iocb, fd, iov, iovcnt, offset);
  iocb->data = request.get();

  ret = io_submit(ioctx, 1, &iocb);
  if (ret < 0) {
    perror("io_submit failed");
    exit(EXIT_FAILURE);
  } else if (ret == 0) {
    perror("io_submit failed to submit even 1 event");
    exit(EXIT_FAILURE);
  }
}

void BlockStore::submit_read(const std::shared_ptr<IoRequest> &request, struct iocb *iocb) {
  uint32_t iovcnt = request->buffers.size();
  uint32_t offset = request->buffers.front()->lbn * opts.block_size;
  struct iovec iov[iovcnt];
  uint32_t i;

  i = 0;
  for (const auto &buffer: request->buffers) {
    assert(!flag_marked(buffer, B_UPTODATE));

    iov[i].iov_base = buffer->buf;
    iov[i].iov_len = opts.block_size;
    ++i;
  }
  
  io_prep_preadv(iocb, fd, iov, iovcnt, offset);
  iocb->data = request.get();

  if (io_submit(ioctx, 1, &iocb) < 0) {
    perror("io_submit failed");
    exit(EXIT_FAILURE);
  }
}

// TODO: right now, the io is submitted once for each io_request
//       this could be slow. But for some reason, submitting multiple
//       io does not work. Need to fix it later.
void BlockStore::submit_routine() {
  std::shared_ptr<IoRequest> request;
  struct iocb *iocb;
  
  iocb = (struct iocb *)malloc(sizeof(struct iocb));

  while (true) {
    request = io_requests.pop();

    // Signal that it's time to exit
    if (request == nullptr) {
      break;
    }

    assert(request->status == IO_IN_PROGRESS);

    if (request->op == OP_WRITE) {
      submit_write(request, iocb);
    } else if (request->op == OP_READ) {
      submit_read(request, iocb);
    } else {
      assert(0);
    }

    ++num_in_progress;
  }
}

void BlockStore::reap_routine() {
  struct io_event *events;
  struct io_event event;
  struct timespec timeout;
  IoRequest *request;
  int num_events;

  events = (struct io_event *)malloc(sizeof(struct io_event) * opts.max_num_event);
  timeout.tv_sec = 0;
  timeout.tv_nsec = 100000000;

  while (true) {
    if (num_in_progress == 0) {
      if (!running) {
        break;
      } else {
        std::this_thread::yield();
        continue;
      }
    }

    num_events = io_getevents(ioctx, opts.min_num_event, opts.max_num_event, events, &timeout);

    if (num_events < 0) {
      perror("io_getevents failed");
      exit(EXIT_FAILURE);
    } else if (num_events == 0) {
      continue;
    }

    for (uint32_t i = 0; i < num_events; ++i) {
      event = events[i];

      request = static_cast<IoRequest *>(event.data);

      if (request->op == OP_WRITE) {
        //if (!request->post_complete_callback) {
          //fprintf(stderr, "[bs] request(%d) has not registed callback\n", request->buffers.front()->lbn);
        //}
        assert(request->post_complete_callback);
      }

      if (request->post_complete_callback) {
        request->post_complete_callback();
      }
      request->signal_complete();
    }

    num_in_progress -= num_events;
  }
}



void BlockStore::stop() {
  while (!io_requests.empty()) {
    std::this_thread::yield();
  }

  io_requests.push(nullptr);
  submit_thread->join();

  running = false;
  reap_thread->join();

  assert(num_in_progress == 0);
  assert(io_requests.empty());
}

}