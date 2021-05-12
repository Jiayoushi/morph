#include <os/block_store.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <map>
#include <iostream>
#include <string.h>

#include "common/env.h"
#include "common/filename.h"
#include "common/utils.h"

namespace morph {

BlockStore::BlockStore(const std::string &name,
                       BlockStoreOptions o):
    name(name),
    opts(o),
    running(true),
    num_in_progress(0),
    ioctx(0) {
  int oflag;

  oflag = O_CREAT | O_RDWR | O_DIRECT | O_SYNC;
  if (!opts.recover) {
    oflag |= O_TRUNC;
  }

  if (!file_exists(name.c_str())) {
    assert(create_directory(name).is_ok());
  }

  fd = open(block_store_file_name(name).c_str(), oflag);
  if (fd < 0) {
    perror("failed to open file to store data");
    assert(0);
  }

  if (fallocate(fd, 0, 0, opts.block_size * opts.TOTAL_BLOCKS) < 0) {
    perror("failed to fallocate");
    assert(0);
  }

  int x;
  if ((x = io_setup(opts.max_num_event, &ioctx)) < 0) {
    std::cerr << "FAILED " << x << std::endl;
    perror("failed to do io_setup");
    assert(0);
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
    assert(0);
  }

  if (io_destroy(ioctx) < 0) {
    perror("failed to io_destory");
    assert(0);
  }

  //fprintf(stderr, "block store exited\n");
}

// TODO: should these belong to the allocator class?
lbn_t BlockStore::allocate_blocks(uint32_t count) {
  return bitmap->allocate_blocks(count);
}

// TODO: same as above
void BlockStore::free_blocks(lbn_t start_block, uint32_t count) {
  bitmap->free_blocks(start_block, count);
}

void BlockStore::submit_request(IoRequest *request) {
  io_requests.push(request);
}

void BlockStore::submit_write(IoRequest *request, struct iocb *iocb) {
  uint32_t offset = request->buffers.front()->lbn * opts.block_size;
  int ret;

  for (Buffer *buffer: request->buffers) {
    if (!flag_marked(buffer, B_DIRTY)) {
      if (++request->completed == request->buffers.size()) {
        //fprintf(stderr, "request(%lu) is ready. exit now\n", request->get_id());
        request->after_complete_callback();
        request->notify_complete();
        break;
      }

      offset += opts.block_size;
      continue;
    }

    io_prep_pwrite(iocb, fd, buffer->buf, buffer->buffer_size, offset);

    iocb->data = request;

    do {
      // TODO: it's definitely better to submit more than 1 request, but somehow
      //   submitting more than one request fails... Need to fix this so that
      //   all these read requests are submitted in one call to io_submit.
      ret = io_submit(ioctx, 1, &iocb);
      if (ret < 0) {
        if (ret == -EAGAIN) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
          continue;
        }
        fprintf(stderr, "io_submit write failed %d\n", ret);
        assert(0);
      } else if (ret == 0) {
        perror("io_submit write failed to submit even 1 event");
        assert(0);
      }
    } while (ret != 1); 
    
    ++num_in_progress;
    offset += opts.block_size;
  }

  //fprintf(stderr, "io_submit write req(%lu) done\n\n",
  //  request->get_id());
}

void BlockStore::submit_read(IoRequest *request, struct iocb *iocb) {
  uint32_t offset = request->buffers.front()->lbn * opts.block_size;
  int ret;

  for (Buffer *buffer: request->buffers) {
    assert(!flag_marked(buffer, B_UPTODATE));

    io_prep_pread(iocb, fd, buffer->buf, buffer->buffer_size, offset);

    iocb->data = request;

    ret = io_submit(ioctx, 1, &iocb);

    if (ret < 0) {
      perror("io_submit read failed");
      assert(0);
    } else if (ret == 0) {
      perror("io_submit read failed to submit even 1 event");
      assert(0);
    }

    ++num_in_progress;
    offset += opts.block_size;
  }

  //fprintf(stderr, "io_submit read done\n");
}

// TODO: right now, the io is submitted once for each io_request
//       this could be slow. But for some reason, submitting multiple
//       io does not work. Need to fix it later.
void BlockStore::submit_routine() {
  IoRequest *request;
  struct iocb *iocb;
  
  iocb = (struct iocb *)malloc(sizeof(struct iocb));

  while (true) {
    request = io_requests.pop();

    // Signal that it's time to exit
    if (request == nullptr) {
      break;
    }

    if (request->op == OP_WRITE) {
      submit_write(request, iocb);
    } else if (request->op == OP_READ) {
      submit_read(request, iocb);
    } else {
      assert(0);
    }
  }

  free(iocb);
}

void BlockStore::reap_routine() {
  struct io_event *events;
  struct io_event event;
  struct timespec timeout;
  IoRequest *request;
  int num_events;
  int error_code;

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

    //fprintf(stderr, "enter io_getevents\n");
    num_events = io_getevents(ioctx, opts.min_num_event, opts.max_num_event, events, &timeout);
    //fprintf(stderr, "leave io_getevents\n");

    if (num_events < 0) {
      if (num_events == -EINTR) {
        assert(0);
        continue;
      } else {
        std::cerr << "io_getevents failed" << std::endl;
        assert(0);
      }
    } else if (num_events == 0) {
      //fprintf(stderr, "[bs] 0 events reaped. num_in_progress=%d\n",
      //  num_in_progress.load());
      continue;
    }
    //fprintf(stderr, "[bs] num_events[%d] num_in_progress=%lu\n", num_events, num_in_progress.load());

    for (uint32_t i = 0; i < num_events; ++i) {
      event = events[i];
      request = (IoRequest *)event.data;
      assert(event.res == request->buffers.front()->buffer_size);

      if (++request->completed == request->buffers.size()) {
        //fprintf(stderr, "[bs] req(%lu) is done, going to call callbacks\n", request->get_id());
        request->after_complete_callback();
      }
    }

    num_in_progress -= num_events;
    //fprintf(stderr, "[bs] reap %d  left %d\n", num_events, num_in_progress.load());
  }

  free(events);
}

void BlockStore::stop() {
  while (!io_requests.empty()) {
    //fprintf(stderr, "[bs] io_requests is not empty, waiting...\n");
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  //fprintf(stderr, "[bs] io_requests are all done, exit...\n");

  io_requests.push(nullptr);
  submit_thread->join();
  //fprintf(stderr, "[bs] submit_thread joined\n");

  running = false;
  reap_thread->join();
  //fprintf(stderr, "[bs] reap_thread joined\n");

  assert(num_in_progress == 0);
  assert(io_requests.empty());
}

}
