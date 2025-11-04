// Coroutine-based custom YCSB client
// Workload format:
//   First line: num_ops,value_size
//   Subsequent lines: r_,_<key> or w_,_<key>
// Outputs:
//   latency file: lines of "<latency_us> <count>" (us accuracy, max 1s)
//   throughput file: one line "<total_ops> <throughput_ops_per_sec>"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <vector>
#include <fstream>
#include <algorithm>
#include <boost/fiber/all.hpp>

#include "client.h"

static inline std::string trim(const std::string &s) {
    size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n')) start++;
    size_t end = s.size();
    while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\r' || s[end - 1] == '\n')) end--;
    return s.substr(start, end - start);
}

int main(int argc, char **argv) {
    if (argc != 7) {
        printf("Usage: %s <path-to-config-file> <path-to-workload-file> <latency-output-file> <throughput-output-file> <num_operations> <is_insert:0|1>\n", argv[0]);
        return 1;
    }

    const char *config_path = argv[1];
    const char *workload_path = argv[2];
    const char *latency_out_path = argv[3];
    const char *throughput_out_path = argv[4];
    uint64_t requested_ops = strtoull(argv[5], NULL, 10);
    int is_insert_flag = atoi(argv[6]);
    bool use_insert = (is_insert_flag != 0);
    if (requested_ops == 0) {
        fprintf(stderr, "Invalid <num_operations>: %s\n", argv[5]);
        return 2;
    }

    // Preload workload
    std::ifstream in(workload_path);
    if (!in.is_open()) {
        fprintf(stderr, "Failed to open workload file: %s\n", workload_path);
        return 2;
    }

    std::string header_line;
    if (!std::getline(in, header_line)) {
        fprintf(stderr, "Workload file is empty: %s\n", workload_path);
        return 2;
    }
    header_line = trim(header_line);

    uint64_t expected_ops = 0;
    uint32_t value_size = 0;
    {
        size_t comma = header_line.find(',');
        if (comma == std::string::npos) {
            fprintf(stderr, "Invalid header line (expected num_ops,value_size): %s\n", header_line.c_str());
            return 2;
        }
        std::string ops_str = trim(header_line.substr(0, comma));
        std::string val_str = trim(header_line.substr(comma + 1));
        expected_ops = strtoull(ops_str.c_str(), NULL, 10);
        value_size = (uint32_t)strtoul(val_str.c_str(), NULL, 10);
        if (expected_ops == 0) {
            fprintf(stderr, "Invalid number of operations: %s\n", ops_str.c_str());
            return 2;
        }
    }

    struct Op { bool is_write; std::string key; };
    std::vector<Op> ops;
    uint64_t max_ops = 0;
    {
        max_ops = std::min<uint64_t>(expected_ops, requested_ops);
        ops.reserve((size_t)max_ops);
    }

    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty()) continue;
        char opch = line[0];
        size_t delim = line.find("_,_");
        if (delim == std::string::npos || delim + 3 > line.size()) continue;
        std::string key = line.substr(delim + 3);
        bool is_write = (opch == 'w' || opch == 'W');
        ops.push_back(Op{is_write, key});
        if (ops.size() >= max_ops) break;
    }
    in.close();

    if (ops.empty()) {
        fprintf(stderr, "No operations parsed from workload file: %s\n", workload_path);
        return 2;
    }

    // Load config
    int ret = 0;
    GlobalConfig config;
    ret = load_config(config_path, &config);
    assert(ret == 0);
    printf("running with %d coros\n", config.num_coroutines);

    // Bind main thread to configured core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(config.main_core_id, &cpuset);
    ret = sched_setaffinity(0, sizeof(cpuset), &cpuset);
    assert(ret == 0);
    ret = sched_getaffinity(0, sizeof(cpuset), &cpuset);
    for (int i = 0; i < sysconf(_SC_NPROCESSORS_CONF); i++) {
        if (CPU_ISSET(i, &cpuset)) {
            printf("main process running on core: %d\n", i);
        }
    }

    // Start client and polling thread
    Client client(&config);
    pthread_t polling_tid = client.start_polling_thread();

    // Latency histogram and coroutine setup
    const uint32_t kMaxLatencyUs = 1000000;
    std::vector<uint64_t> write_hist(kMaxLatencyUs + 1, 0);
    std::vector<uint64_t> read_hist(kMaxLatencyUs + 1, 0);
    uint64_t attempted_ops = 0;
    uint64_t success_ops = 0;
    uint64_t failed_ops = 0;
    uint64_t cas_retry_cnt = 0;
    uint64_t cas_fail_cnt = 0;
    uint64_t failed_ops_due_to_cas = 0;      // writes that retried and ultimately failed
    uint64_t failed_reads_no_match = 0;      // read failures
    uint64_t failed_writes_pre_cas = 0;      // write failures without retry
    uint32_t num_coro = client.num_coroutines_ > 0 ? client.num_coroutines_ : 1;

    // Time the whole run
    struct timeval total_st, total_et;
    gettimeofday(&total_st, NULL);

    // Process in chunks like ycsb_test to bound memory
    const uint32_t kChunkOps = 100000;
    uint64_t ops_done = 0;
    for (size_t off = 0; off < ops.size(); off += kChunkOps) {
        uint32_t this_chunk = (uint32_t)std::min<size_t>(kChunkOps, ops.size() - off);

        // allocate per-chunk arrays
        client.num_local_operations_ = this_chunk;
        client.kv_info_list_ = (KVInfo *)malloc(sizeof(KVInfo) * this_chunk);
        client.kv_req_ctx_list_ = (KVReqCtx *)malloc(sizeof(KVReqCtx) * this_chunk);
        memset(client.kv_info_list_, 0, sizeof(KVInfo) * this_chunk);
        memset(client.kv_req_ctx_list_, 0, sizeof(KVReqCtx) * this_chunk);

        // stage key/value for this chunk from start of input buffer
        uint64_t input_ptr = (uint64_t)client.get_input_buf();
        for (uint32_t i = 0; i < this_chunk; i++) {
            const Op &op = ops[off + i];
            KVInfo *info = &client.kv_info_list_[i];
            KVLogHeader *hdr = (KVLogHeader *)input_ptr;
            hdr->is_valid = true;
            hdr->key_length = (uint16_t)op.key.size();
            hdr->value_length = op.is_write ? value_size : 0;
            char *key_dst = (char *)(input_ptr + sizeof(KVLogHeader));
            memcpy(key_dst, op.key.data(), op.key.size());
            if (op.is_write) {
                char *val_dst = key_dst + op.key.size();
                memset(val_dst, 'v', value_size);
                KVLogTail *tail = (KVLogTail *)(val_dst + value_size);
                tail->op = use_insert ? KV_OP_INSERT : KV_OP_UPDATE;
                input_ptr = (uint64_t)(tail + 1);
            } else {
                KVLogTail *tail = (KVLogTail *)(key_dst + op.key.size());
                tail->op = KV_OP_INSERT;
                input_ptr = (uint64_t)(tail + 1);
            }
            info->l_addr = (void *)((uint64_t)hdr);
            info->lkey = client.get_input_buf_lkey();
            info->key_len = hdr->key_length;
            info->value_len = hdr->value_length;

            // initialize req ctx
            KVReqCtx *req = &client.kv_req_ctx_list_[i];
            req->kv_info = info;
            req->lkey = client.get_local_buf_mr()->lkey;
            req->kv_modify_pr_cas_list.resize(1);
            int num_idx_rep = client.get_num_idx_rep();
            int num_rep = client.get_num_rep();
            if (num_idx_rep > 0) {
                req->kv_modify_bk_0_cas_list.resize(num_idx_rep - 1);
                req->kv_modify_bk_1_cas_list.resize(num_idx_rep - 1);
            }
            req->log_commit_addr_list.resize(num_rep);
            req->write_unused_addr_list.resize(num_rep);
            char key_buf[128] = {0};
            memcpy(key_buf, (void *)((uint64_t)(info->l_addr) + sizeof(KVLogHeader)), info->key_len);
            req->key_str = std::string(key_buf);
            req->req_type = op.is_write ? (use_insert ? KV_REQ_INSERT : KV_REQ_UPDATE) : KV_REQ_SEARCH;
        }

        // partition and run fibers on this chunk
        uint32_t base = this_chunk / num_coro;
        uint32_t rem = this_chunk % num_coro;
        auto fiber_body = [&](uint32_t coro_id, uint32_t st_idx, uint32_t count) {
            if (count == 0) return;
            client.init_kvreq_space(coro_id, st_idx, count);
            for (uint32_t i = 0; i < count; i++) {
                KVReqCtx *ctx = &client.kv_req_ctx_list_[st_idx + i];
                ctx->coro_id = coro_id;
                struct timeval st, et;
                attempted_ops++;
                if (ctx->req_type == KV_REQ_INSERT) {
                    gettimeofday(&st, NULL);
                    bool had_retry = false;
                    int rc;
                    do {
                        rc = (ctx->req_type == KV_REQ_INSERT) ? client.kv_insert(ctx) : client.kv_update(ctx);
                        if (rc == KV_OPS_FAIL_REDO) { cas_retry_cnt++; had_retry = true; }
                    } while (rc == KV_OPS_FAIL_REDO);
                    gettimeofday(&et, NULL);
                    if (rc == KV_OPS_FAIL_RETURN) { if (had_retry) { cas_fail_cnt++; failed_ops_due_to_cas++; } else { failed_writes_pre_cas++; } failed_ops++; continue; }
                    if (rc != KV_OPS_SUCCESS) { failed_ops++; continue; }
                } else {
                    gettimeofday(&st, NULL);
                    void *res = client.kv_search(ctx);
                    gettimeofday(&et, NULL);
                    if (res == NULL) { failed_ops++; failed_reads_no_match++; continue; }
                }
                uint64_t lat_us = (uint64_t)(et.tv_sec - st.tv_sec) * 1000000ULL + (uint64_t)(et.tv_usec - st.tv_usec);
                if (lat_us > kMaxLatencyUs) lat_us = kMaxLatencyUs;
                if (ctx->req_type == KV_REQ_SEARCH) read_hist[(size_t)lat_us]++; else write_hist[(size_t)lat_us]++;
                success_ops++;
            }
        };

        // record base successful ops from histogram before running this chunk
        uint64_t base_succ = 0;
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) base_succ += (write_hist[us] + read_hist[us]);

        std::vector<boost::fibers::fiber> fbs;
        fbs.reserve(num_coro);
        uint32_t cur = 0;
        for (uint32_t c = 0; c < num_coro; ++c) {
            uint32_t cnt = base + (c < rem ? 1u : 0u);
            uint32_t st = cur;
            cur += cnt;
            fbs.emplace_back(fiber_body, c, st, cnt);
        }
        for (auto &fb : fbs) fb.join();

        // ops_done counts only new successful operations added by this chunk
        uint64_t new_succ = 0;
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) new_succ += (write_hist[us] + read_hist[us]);
        ops_done += (new_succ - base_succ);

        free(client.kv_info_list_);
        free(client.kv_req_ctx_list_);
        client.kv_info_list_ = NULL;
        client.kv_req_ctx_list_ = NULL;
    }

    gettimeofday(&total_et, NULL);
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);

    // Write latency file: write histogram, separator, read histogram (comma-separated: latency,count)
    {
        std::ofstream lat_out(latency_out_path, std::ios::out | std::ios::trunc);
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) {
            uint64_t cnt = write_hist[us];
            if (cnt > 0) lat_out << us << "," << cnt << "\n";
        }
        lat_out << "******************************\n";
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) {
            uint64_t cnt = read_hist[us];
            if (cnt > 0) lat_out << us << "," << cnt << "\n";
        }
    }

    // Throughput (CSV): tpt,success_ops,total_ops,workload_duration_sec
    double elapsed_sec = ((double)(total_et.tv_sec - total_st.tv_sec)) + ((double)(total_et.tv_usec - total_st.tv_usec)) / 1e6;
    if (elapsed_sec <= 0.0) elapsed_sec = 1e-9;
    // ops_done accumulated across chunks
    double tpt = success_ops / elapsed_sec;
    {
        std::ofstream tpt_out(throughput_out_path, std::ios::out | std::ios::trunc);
        tpt_out << tpt << "," << success_ops << "," << attempted_ops << "," << elapsed_sec << "\n";
    }

    printf("Completed %llu ops in %.3f sec (%.2f ops/s)\n",
           (unsigned long long)ops_done, elapsed_sec, tpt);

    // Write CAS stats next to throughput file
    {
        std::string tpt_path(throughput_out_path);
        size_t slash = tpt_path.find_last_of("/\\");
        std::string dir = (slash == std::string::npos) ? std::string("") : tpt_path.substr(0, slash + 1);
        std::string cas_path = dir + std::string("cas_stats_") + std::to_string(config.server_id) + std::string(".csv");
        std::ofstream cas_out(cas_path.c_str(), std::ios::out | std::ios::trunc);
        cas_out << "attempted_ops,success_ops,failed_ops,failed_reads_no_match,failed_writes_pre_cas,failed_cas,retry_cas,failed_ops_due_to_cas,update_cas_soft_fail\n";
        cas_out << attempted_ops << "," << success_ops << "," << failed_ops << ","
                << failed_reads_no_match << "," << failed_writes_pre_cas << ","
                << cas_fail_cnt << "," << cas_retry_cnt << "," << failed_ops_due_to_cas << ","
                << client.stat_update_cas_soft_fail_ << "\n";
    }

    return 0;
}
