#include <os/journal.h>

#include <unistd.h>
#include <fcntl.h>
#include <cassert>
#include <common/crc.h>
#include <rpc/msgpack.hpp>
#include <spdlog/fmt/bundled/printf.h>

namespace morph {

void Transaction::prepare() {
  std::stringstream header_ss;
  std::stringstream payload_ss;

  assert(state == TXN_COMPLETE);
  assert(header_buffer.empty());
  assert(payload_buffer.empty());

  clmdep_msgpack::pack(payload_ss, log_handles);
  // TODO: use move?
  payload_buffer = payload_ss.str();

  header.length = payload_buffer.size();
  header.checksum = crc32_fast(payload_buffer.c_str(), payload_buffer.size());
  header.type = TXN_FULL;
  clmdep_msgpack::pack(header_ss, log_handles);
  header_buffer = header_ss.str();

  assert(payload_buffer.size() + header_buffer.size() <= JOURNAL_BLOCK_SIZE);
}

Journal::Journal():
  running(true),
  journal_file_size(0),
  journal_file_sequence_number(0) {
  
  open_new_journal_file();

  open_txn = std::make_unique<Transaction>();

  sync_to_local_thread = std::make_unique<std::thread>(&Journal::sync_to_local_storage_routine, this);
  close_txn_thread = std::make_unique<std::thread>(&Journal::close_transaction_routine, this);
}

Journal::~Journal() {
  running = false;

  sync_to_local_thread->join();
  close_txn_thread->join();

  close(fd);
}

// TODO: if the transactions in memory reach the limit, flush to disk first.
LogHandle *Journal::start() {
  return open_txn->allocate_log_handle();
}

void Journal::end(LogHandle *log_handle) {
  log_handle->txn->complete_log_handle();
}

void Journal::close_transaction_routine() {
  bool closed;

  while (running) {
    std::this_thread::sleep_for(std::chrono::seconds(JOURNAL_TRANSACTION_CLOSE_INTERVAL));

    // Close the current active transaction
    closed = open_txn->try_close();
    if (closed) {
      closed_txns.push(std::move(open_txn));
      open_txn = std::make_unique<Transaction>();
    }
  }
}

void Journal::sync_to_local_storage_routine() {
  std::shared_ptr<Transaction> txn;
  uint16_t current_size = 0;
  uint16_t block_size = 0;
  char *block;
  size_t header_size;
  size_t payload_size;
  ssize_t total_written;
  ssize_t written;

  block = (char *)aligned_alloc(512, JOURNAL_BLOCK_SIZE);
  if (block == nullptr) {
    perror("Failed to allocate in memory block for journal");
    exit(EXIT_FAILURE);
  }

  while (running) {
    std::unique_lock<std::mutex> lck(sync_to_local_mutex);

    sync_to_local_cv.wait_for(lck, std::chrono::seconds(JOURNAL_TRANSACTION_SYNC_INTERVAL));

    // This thread is the only thread that will pop txns from the closed txns queue
    // so it is safe to peek and then pop
    txn = closed_txns.peek();
    if (txn != nullptr && txn->state == TXN_COMPLETE) {
      if (txn->header.length == 0) {
        txn->prepare();
      }

      header_size = txn->header_buffer.size();
      payload_size = txn->payload_buffer.size();
      if (current_size + txn->header.length <= JOURNAL_BLOCK_SIZE) {
        closed_txns.wait_and_pop();
        memcpy(block + block_size, txn->header_buffer.c_str(), header_size);
        memcpy(block + block_size + payload_size, txn->payload_buffer.c_str(), payload_size);
        block_size += header_size + payload_size;
      } else {
        total_written = 0;

        while (total_written != JOURNAL_BLOCK_SIZE) {
          written = write(fd, block + total_written, JOURNAL_BLOCK_SIZE - total_written);
          if (written < 0) {
            perror("Failed to write");
            exit(EXIT_FAILURE);
          }
          total_written += written;
        }

        if (fsync(fd) < 0) {
          perror("Failed to fsync");
          exit(EXIT_FAILURE);
        }

        journal_file_size += total_written;
        if (journal_file_size >= JOURNAL_FILE_SIZE_THRESHOLD) {
          switch_to_new_journal_file();
        }
      }
    }
  }

  free(block);
}

void Journal::open_new_journal_file() {
  const std::string journal_file = fmt::sprintf("%s/journal-%d", JOURNAL_DIRECTORY, journal_file_sequence_number);
  fd = open(journal_file.c_str(),
            O_CREAT | O_RDWR | O_DIRECT | O_TRUNC,
            S_IRUSR | S_IWUSR);
  if (fd < 0) {
    perror(fmt::sprintf("Failed to open journal file %s", journal_file.c_str()).c_str());
    exit(EXIT_FAILURE);
  }

  ++journal_file_sequence_number;
  journal_file_size = 0;
}

void Journal::switch_to_new_journal_file() {
  if (close(fd) < 0) {
    perror("Failed to close journal file.");
    exit(EXIT_FAILURE);
  }
  open_new_journal_file();
}

}