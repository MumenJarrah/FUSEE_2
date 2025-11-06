// Multi-thread custom YCSB client (no barriers)
// CLI:
//   ycsb_custom_multi_client <config.json> <workload.txt> <latency_out.csv> <throughput_out.csv> <num_operations> <is_insert:0|1> <num_threads>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <atomic>
#include <mutex>
#include <boost/fiber/all.hpp>

#include "client.h"

static inline std::string trim(const std::string &s) {
    size_t st = 0, ed = s.size();
    while (st < ed && (s[st] == ' ' || s[st] == '\t' || s[st] == '\r' || s[st] == '\n')) st++;
    while (ed > st && (s[ed-1] == ' ' || s[ed-1] == '\t' || s[ed-1] == '\r' || s[ed-1] == '\n')) ed--;
    return s.substr(st, ed - st);
}

struct Op { bool is_write; std::string key; };

struct ThreadArgs {
    GlobalConfig base_conf;
    const std::vector<Op> *ops;
    size_t st_idx;
    size_t ed_idx;
    bool use_insert;
    uint32_t value_size;
    // shared aggregation
    uint64_t *write_hist;
    uint64_t *read_hist;
    uint32_t hist_cap;
    std::atomic<uint64_t> *attempted;
    std::atomic<uint64_t> *success;
    std::atomic<uint64_t> *failed;
    std::mutex *agg_mu;
    // timing aggregation
    std::atomic<uint64_t> *min_start_us;
    std::atomic<uint64_t> *max_end_us;
};

static inline uint64_t now_us() {
    timeval tv; gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}

static void * thread_main(void *argp) {
    ThreadArgs *ta = (ThreadArgs *)argp;
    GlobalConfig conf = ta->base_conf;
    // adjust per-thread server_id/core affinity based on st_idx offset
    // we use st_idx as a proxy thread ordinal passed from launcher
    // the launcher sets conf.main_core_id/poll_core_id already staggered

    // bind core happens inside Client via sched_setaffinity (like other clients)
    Client client(&conf);
    pthread_t polling_tid = client.start_polling_thread();

    const uint32_t kMaxLatencyUs = ta->hist_cap; // 1,000,000

    // Build client's internal request arrays to use fiber-based issuance
    size_t N = ta->ed_idx - ta->st_idx;
    client.num_local_operations_ = (uint32_t)N;
    client.kv_info_list_ = (KVInfo *)malloc(sizeof(KVInfo) * N);
    client.kv_req_ctx_list_ = (KVReqCtx *)malloc(sizeof(KVReqCtx) * N);
    memset(client.kv_info_list_, 0, sizeof(KVInfo) * N);
    memset(client.kv_req_ctx_list_, 0, sizeof(KVReqCtx) * N);

    uint8_t *input_ptr = (uint8_t *)client.get_input_buf();
    uint64_t used_len = 0;
    for (size_t i = 0; i < N; ++i) {
        const Op &op = (*ta->ops)[ta->st_idx + i];
        KVLogHeader *hdr = (KVLogHeader *)input_ptr;
        hdr->is_valid = true;
        hdr->key_length = (uint16_t)op.key.size();
        hdr->value_length = op.is_write ? ta->value_size : 0;
        char *key_dst = (char *)(input_ptr + sizeof(KVLogHeader));
        memcpy(key_dst, op.key.data(), op.key.size());
        if (op.is_write) {
            char *val_dst = key_dst + op.key.size();
            if (hdr->value_length) memset(val_dst, 'v', hdr->value_length);
            KVLogTail *tail = (KVLogTail *)(val_dst + hdr->value_length);
            tail->op = ta->use_insert ? KV_OP_INSERT : KV_OP_UPDATE;
        }
        // fill kv_info
        client.kv_info_list_[i].l_addr = (void *)input_ptr;
        client.kv_info_list_[i].lkey   = client.get_input_buf_lkey();
        client.kv_info_list_[i].key_len = hdr->key_length;
        client.kv_info_list_[i].value_len = hdr->value_length;

        // minimally init req ctx like init_kv_req_ctx
        KVReqCtx &rc = client.kv_req_ctx_list_[i];
        rc.kv_info = &client.kv_info_list_[i];
        rc.lkey = client.get_input_buf_lkey();
        rc.kv_modify_pr_cas_list.resize(1);
        // num_idx_rep is in conf
        rc.kv_modify_bk_0_cas_list.resize(conf.num_idx_rep - 1);
        rc.kv_modify_bk_1_cas_list.resize(conf.num_idx_rep - 1);
        rc.log_commit_addr_list.resize(conf.num_replication);
        rc.write_unused_addr_list.resize(conf.num_replication);
        rc.key_str = op.key;
        rc.req_type = op.is_write ? (ta->use_insert ? KV_REQ_INSERT : KV_REQ_UPDATE) : KV_REQ_SEARCH;

        // advance input pointer
        uint32_t total_len = sizeof(KVLogHeader) + hdr->key_length + hdr->value_length + (op.is_write ? sizeof(KVLogTail) : 0);
        input_ptr += total_len;
        used_len += total_len;
        // naive overflow warning
        if (used_len >= CLINET_INPUT_BUF_LEN) {
            // best-effort guard; continue staging (may overwrite) but warn
            // In production, chunking should be implemented.
            // fprintf(stderr, "Warning: input buffer overflow potential (used %llu)\n", (unsigned long long)used_len);
        }
    }

    // Fiber-based issuance similar to ycsb_test: split across coroutines
    int num_coro = conf.num_coroutines > 0 ? conf.num_coroutines : 1;
    std::vector<boost::fibers::fiber> fibers;
    fibers.reserve(num_coro);
    std::vector<uint64_t> local_success(num_coro, 0), local_failed(num_coro, 0);
    uint64_t start_us = now_us();
    volatile bool should_stop = false;
    uint32_t per = (uint32_t)(N / num_coro);
    uint32_t rem = (uint32_t)(N % num_coro);
    uint32_t off = 0;
    for (int c = 0; c < num_coro; ++c) {
        uint32_t cnt = per + (c < (int)rem ? 1u : 0u);
        uint32_t st = off; off += cnt;
        fibers.emplace_back([&, c, st, cnt]() {
            // prepare per-coro spaces
            client.init_kvreq_space((uint32_t)c, st, cnt);
            uint64_t succ = 0, fail = 0;
            for (uint32_t i = 0; i < cnt; ++i) {
                KVReqCtx *ctx = &client.kv_req_ctx_list_[st + i];
                ctx->coro_id = (uint32_t)c;
                ctx->should_stop = &should_stop;
                timeval stv, etv; gettimeofday(&stv, NULL);
                bool ok = true;
                if (ctx->req_type == KV_REQ_SEARCH) {
                    void *res = client.kv_search(ctx);
                    ok = (res != NULL);
                } else if (ctx->req_type == KV_REQ_INSERT) {
                    int rc; do { rc = client.kv_insert(ctx); } while (rc == KV_OPS_FAIL_REDO);
                    ok = (rc == KV_OPS_SUCCESS);
                } else if (ctx->req_type == KV_REQ_UPDATE) {
                    int rc = client.kv_update(ctx);
                    ok = (rc == KV_OPS_SUCCESS);
                } else {
                    // ignore others
                    ok = false;
                }
                gettimeofday(&etv, NULL);
                if (ok) {
                    uint64_t lat_us = (uint64_t)(etv.tv_sec - stv.tv_sec) * 1000000ULL + (uint64_t)(etv.tv_usec - stv.tv_usec);
                    if (lat_us > kMaxLatencyUs) lat_us = kMaxLatencyUs;
                    std::lock_guard<std::mutex> g(*ta->agg_mu);
                    if (ctx->req_type == KV_REQ_SEARCH) ta->read_hist[lat_us]++; else ta->write_hist[lat_us]++;
                    succ++;
                } else {
                    fail++;
                }
            }
            local_success[c] = succ; local_failed[c] = fail;
        });
    }
    for (auto &fb : fibers) fb.join();
    uint64_t end_us = now_us();
    // aggregate attempted/success/failed
    uint64_t succ_sum = 0, fail_sum = 0;
    for (int c = 0; c < num_coro; ++c) { succ_sum += local_success[c]; fail_sum += local_failed[c]; }
    ta->success->fetch_add(succ_sum, std::memory_order_relaxed);
    ta->failed->fetch_add(fail_sum, std::memory_order_relaxed);
    ta->attempted->fetch_add(N, std::memory_order_relaxed);

    // update global min/max without barriers
    uint64_t cur_min = ta->min_start_us->load();
    while (start_us < cur_min && !ta->min_start_us->compare_exchange_weak(cur_min, start_us)) {}
    uint64_t cur_max = ta->max_end_us->load();
    while (end_us > cur_max && !ta->max_end_us->compare_exchange_weak(cur_max, end_us)) {}

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);
    return NULL;
}

int main(int argc, char **argv) {
    if (argc != 8) {
        printf("Usage: %s <config.json> <workload.txt> <latency_out.csv> <throughput_out.csv> <num_operations> <is_insert:0|1> <num_threads>\n", argv[0]);
        return 1;
    }

    const char *config_path = argv[1];
    const char *workload_path = argv[2];
    const char *lat_out_path = argv[3];
    const char *tpt_out_path = argv[4];
    uint64_t num_ops_req = strtoull(argv[5], NULL, 10);
    bool use_insert = atoi(argv[6]) != 0;
    int num_threads = atoi(argv[7]);
    if (num_threads <= 0) { printf("num_threads must be > 0\n"); return 1; }

    // load config
    GlobalConfig base_conf; int ret = load_config(config_path, &base_conf); assert(ret == 0);

    // Preload workload
    std::ifstream in(workload_path); if (!in.is_open()) { fprintf(stderr, "Failed to open workload file: %s\n", workload_path); return 2; }
    std::string header_line; if (!std::getline(in, header_line)) { fprintf(stderr, "Empty workload file\n"); return 2; }
    header_line = trim(header_line);
    size_t comma = header_line.find(','); if (comma == std::string::npos) { fprintf(stderr, "Invalid header line\n"); return 2; }
    uint64_t expected_ops = strtoull(trim(header_line.substr(0, comma)).c_str(), NULL, 10);
    uint32_t value_size = (uint32_t)strtoul(trim(header_line.substr(comma+1)).c_str(), NULL, 10);

    std::vector<Op> ops; ops.reserve(std::min(expected_ops, num_ops_req));
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line); if (line.empty()) continue;
        char opch = line[0]; size_t delim = line.find("_,_"); if (delim == std::string::npos || delim + 3 > line.size()) continue;
        std::string key = line.substr(delim + 3);
        bool is_write = (opch == 'w' || opch == 'W');
        ops.push_back(Op{is_write, key});
        if (ops.size() >= num_ops_req) break;
    }
    in.close();
    if (ops.empty()) { fprintf(stderr, "No operations parsed\n"); return 2; }

    // Shared histograms and counters
    const uint32_t kMaxLatencyUs = 1000000;
    std::vector<uint64_t> write_hist(kMaxLatencyUs + 1, 0), read_hist(kMaxLatencyUs + 1, 0);
    std::atomic<uint64_t> attempted{0}, success{0}, failed{0};
    std::mutex agg_mu;
    std::atomic<uint64_t> min_start_us{~(uint64_t)0};
    std::atomic<uint64_t> max_end_us{0};

    // Launch threads: each thread executes the full ops vector (no splitting)
    std::vector<pthread_t> tids(num_threads);
    std::vector<ThreadArgs> tas(num_threads);
    for (int i = 0; i < num_threads; i++) {
        tas[i].base_conf = base_conf;
        // stagger cores and server_id like ycsb
        tas[i].base_conf.main_core_id = base_conf.main_core_id + i * 2;
        tas[i].base_conf.poll_core_id = base_conf.poll_core_id + i * 2;
        tas[i].base_conf.server_id    = base_conf.server_id + i;
        tas[i].ops = &ops; tas[i].st_idx = 0; tas[i].ed_idx = ops.size();
        tas[i].use_insert = use_insert;
        tas[i].value_size = value_size;
        tas[i].write_hist = write_hist.data(); tas[i].read_hist = read_hist.data(); tas[i].hist_cap = kMaxLatencyUs;
        tas[i].attempted = &attempted; tas[i].success = &success; tas[i].failed = &failed; tas[i].agg_mu = &agg_mu;
        tas[i].min_start_us = &min_start_us; tas[i].max_end_us = &max_end_us;
        pthread_create(&tids[i], NULL, thread_main, &tas[i]);
    }

    for (int i = 0; i < num_threads; i++) pthread_join(tids[i], NULL);

    // latency CSV: write hist, separator, read hist
    {
        std::ofstream lat_out(lat_out_path, std::ios::out | std::ios::trunc);
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) if (write_hist[us] > 0) lat_out << us << "," << write_hist[us] << "\n";
        lat_out << "******************************\n";
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) if (read_hist[us] > 0) lat_out << us << "," << read_hist[us] << "\n";
    }

    // throughput CSV: tpt,success,attempted,duration
    double duration_sec = (max_end_us.load() - min_start_us.load()) / 1e6;
    if (duration_sec <= 0.0) duration_sec = 1e-9;
    double tpt = success.load() / duration_sec;
    {
        std::ofstream tpt_out(tpt_out_path, std::ios::out | std::ios::trunc);
        tpt_out << tpt << "," << success.load() << "," << attempted.load() << "," << duration_sec << "\n";
    }

    printf("Completed %llu ops in %.3f sec (%.2f ops/s)\n",
           (unsigned long long)attempted.load(), duration_sec, tpt);
    return 0;
}
