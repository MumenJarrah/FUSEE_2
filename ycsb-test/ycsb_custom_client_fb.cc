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
    if (argc != 5) {
        printf("Usage: %s <path-to-config-file> <path-to-workload-file> <latency-output-file> <throughput-output-file>\n", argv[0]);
        return 1;
    }

    const char *config_path = argv[1];
    const char *workload_path = argv[2];
    const char *latency_out_path = argv[3];
    const char *throughput_out_path = argv[4];

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
    ops.reserve(expected_ops);

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
        if (ops.size() >= expected_ops) break;
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

    // Build client KV array from preloaded ops
    client.num_total_operations_ = 0;
    client.num_local_operations_ = (uint32_t)ops.size();
    client.kv_info_list_ = (KVInfo *)malloc(sizeof(KVInfo) * client.num_local_operations_);
    memset(client.kv_info_list_, 0, sizeof(KVInfo) * client.num_local_operations_);

    uint64_t input_ptr = (uint64_t)client.get_input_buf();
    for (size_t i = 0; i < ops.size(); i++) {
        const Op &op = ops[i];
        KVInfo *info = &client.kv_info_list_[i];
        // layout: [header][key][value?][tail]
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
            tail->op = KV_OP_INSERT;
            input_ptr = (uint64_t)(tail + 1);
        } else {
            KVLogTail *tail = (KVLogTail *)(key_dst + op.key.size());
            tail->op = KV_OP_INSERT; // not used for reads
            input_ptr = (uint64_t)(tail + 1);
        }
        info->l_addr = (void *)((uint64_t)hdr);
        info->lkey = client.get_input_buf_lkey();
        info->key_len = hdr->key_length;
        info->value_len = hdr->value_length;

        // No need to init KVReqCtx; we will use KVInfo* APIs per op
    }

    // Latency histogram
    const uint32_t kMaxLatencyUs = 1000000;
    std::vector<uint64_t> lat_hist(kMaxLatencyUs + 1, 0);

    // Partition ops across fibers
    uint32_t num_coro = client.num_coroutines_ > 0 ? client.num_coroutines_ : 1;
    uint32_t base = client.num_local_operations_ / num_coro;
    uint32_t rem = client.num_local_operations_ % num_coro;

    auto fiber_body = [&](uint32_t coro_id, uint32_t st_idx, uint32_t count) {
        if (count == 0) return;
        for (uint32_t i = 0; i < count; i++) {
            KVInfo *info = &client.kv_info_list_[st_idx + i];
            struct timeval st, et;
            // Determine op type from value_len (writes have value)
            bool is_write = (info->value_len != 0);
            if (is_write) {
                gettimeofday(&st, NULL);
                (void)client.kv_insert(info);
                gettimeofday(&et, NULL);
            } else {
                gettimeofday(&st, NULL);
                (void)client.kv_search(info);
                gettimeofday(&et, NULL);
            }
            uint64_t lat_us = (uint64_t)(et.tv_sec - st.tv_sec) * 1000000ULL + (uint64_t)(et.tv_usec - st.tv_usec);
            if (lat_us > kMaxLatencyUs) lat_us = kMaxLatencyUs;
            lat_hist[(size_t)lat_us]++;
        }
    };

    // Time the whole run and launch fibers
    struct timeval total_st, total_et;
    gettimeofday(&total_st, NULL);
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
    gettimeofday(&total_et, NULL);

    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);

    // Write latency histogram
    {
        std::ofstream lat_out(latency_out_path, std::ios::out | std::ios::trunc);
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) {
            uint64_t cnt = lat_hist[us];
            if (cnt > 0) {
                lat_out << us << " " << cnt << "\n";
            }
        }
    }

    // Throughput
    double elapsed_sec = ((double)(total_et.tv_sec - total_st.tv_sec)) + ((double)(total_et.tv_usec - total_st.tv_usec)) / 1e6;
    if (elapsed_sec <= 0.0) elapsed_sec = 1e-9;
    uint64_t ops_done = client.num_local_operations_;
    double tpt = ops_done / elapsed_sec;
    {
        std::ofstream tpt_out(throughput_out_path, std::ios::out | std::ios::trunc);
        tpt_out << ops_done << " " << tpt << "\n";
    }

    printf("Completed %llu ops in %.3f sec (%.2f ops/s)\n",
           (unsigned long long)ops_done, elapsed_sec, tpt);

    return 0;
}
