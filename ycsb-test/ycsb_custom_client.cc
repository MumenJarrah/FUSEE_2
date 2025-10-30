// Simple YCSB-style custom client that reads a workload file
// Format:
//   First line: num_ops,value_size
//   Remaining lines: each line is either "r_,_<key>" or "w_,_<key>"
// Outputs:
//   latency_<workload_basename>.txt  (one latency per line, in microseconds)
//   throughput_<workload_basename>.txt (one line: "<total_ops> <throughput_ops_per_sec>")

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <algorithm>

#include "client.h"

static inline std::string trim(const std::string &s) {
    size_t start = 0;
    while (start < s.size() && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n')) start++;
    size_t end = s.size();
    while (end > start && (s[end - 1] == ' ' || s[end - 1] == '\t' || s[end - 1] == '\r' || s[end - 1] == '\n')) end--;
    return s.substr(start, end - start);
}

static inline void prepare_kvinfo(Client &client, const std::string &key, uint32_t value_size,
                                  bool is_write, bool use_insert, KVInfo *out_kv) {
    uint8_t *base = (uint8_t *)client.get_input_buf();

    KVLogHeader *header = (KVLogHeader *)base;
    header->is_valid = true;
    header->key_length = (uint16_t)key.size();
    header->value_length = is_write ? value_size : 0;

    char *key_dst = (char *)(base + sizeof(KVLogHeader));
    memcpy(key_dst, key.data(), key.size());

    if (is_write) {
        char *val_dst = key_dst + key.size();
        memset(val_dst, 'v', value_size);
        KVLogTail *tail = (KVLogTail *)(val_dst + value_size);
        tail->op = use_insert ? KV_OP_INSERT : KV_OP_UPDATE;
    }

    out_kv->l_addr = base;
    out_kv->lkey = client.get_input_buf_lkey();
    out_kv->key_len = (uint32_t)key.size();
    out_kv->value_len = is_write ? value_size : 0;
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

    // Open and fully read workload file (no disk I/O during ops)
    std::ifstream in(workload_path);
    if (!in.is_open()) {
        fprintf(stderr, "Failed to open workload file: %s\n", workload_path);
        return 2;
    }

    // Parse first line: num_ops,value_size
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

    // Preload operations into memory
    struct Op { bool is_write; std::string key; };
    std::vector<Op> ops;
    uint64_t max_ops = std::min<uint64_t>(expected_ops, requested_ops);
    ops.reserve((size_t)max_ops);
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty()) continue;
        char opch = line[0];
        size_t delim = line.find("_,_");
        if (delim == std::string::npos || delim + 3 > line.size()) {
            // Skip malformed lines
            continue;
        }
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

    // Load client config after file I/O
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

    // Build client and start polling thread
    Client client(&config);
    pthread_t polling_tid = client.start_polling_thread();

    // Prepare latency histogram (microsecond accuracy, cap at 1s)
    const uint32_t kMaxLatencyUs = 1000000; // 1 second in microseconds
    std::vector<uint64_t> lat_hist(kMaxLatencyUs + 1, 0);

    // Time the whole run
    struct timeval total_st, total_et;
    gettimeofday(&total_st, NULL);

    // Execute preloaded operations (skip failed ops for latency/throughput)
    uint64_t attempted_ops = 0;
    uint64_t success_ops = 0;
    uint64_t failed_ops = 0;
    uint64_t cas_retry_cnt = 0;
    uint64_t cas_fail_cnt = 0;
    uint64_t failed_ops_due_to_cas = 0;      // subset of failed_ops: writes that retried and ultimately failed
    uint64_t failed_reads_no_match = 0;      // read failures
    uint64_t failed_writes_pre_cas = 0;      // write failures that did not retry (pre-CAS failures)
    for (size_t i = 0; i < ops.size(); i++) {
        const Op &op = ops[i];
        KVInfo kv = {};
        prepare_kvinfo(client, op.key, value_size, op.is_write, use_insert, &kv);

        struct timeval st, et;
        bool ok = true;
        attempted_ops++;
        if (op.is_write) {
            gettimeofday(&st, NULL);
            bool had_retry = false;
            int rc;
            do {
                rc = use_insert ? client.kv_insert(&kv) : client.kv_update(&kv);
                if (rc == KV_OPS_FAIL_REDO) {
                    cas_retry_cnt++;
                    had_retry = true;
                }
            } while (rc == KV_OPS_FAIL_REDO);
            gettimeofday(&et, NULL);
            if (rc == KV_OPS_FAIL_RETURN) {
                if (had_retry) { cas_fail_cnt++; failed_ops_due_to_cas++; }
                else { failed_writes_pre_cas++; }
                ok = false;
            } else if (rc != KV_OPS_SUCCESS) {
                ok = false;
            }
        } else {
            gettimeofday(&st, NULL);
            void *res = client.kv_search(&kv);
            gettimeofday(&et, NULL);
            if (res == NULL) { ok = false; failed_reads_no_match++; }
        }
        if (ok) {
            uint64_t lat_us = (uint64_t)(et.tv_sec - st.tv_sec) * 1000000ULL + (uint64_t)(et.tv_usec - st.tv_usec);
            if (lat_us > kMaxLatencyUs) lat_us = kMaxLatencyUs; // cap at 1s bucket
            lat_hist[(size_t)lat_us]++;
            success_ops++;
        } else {
            failed_ops++;
        }
    }

    gettimeofday(&total_et, NULL);
    client.stop_polling_thread();
    pthread_join(polling_tid, NULL);

    // Write latency histogram file: "<latency_us> <count>" per non-zero bucket
    {
        std::ofstream lat_out(latency_out_path, std::ios::out | std::ios::trunc);
        for (uint32_t us = 0; us <= kMaxLatencyUs; ++us) {
            uint64_t cnt = lat_hist[us];
            if (cnt > 0) {
                lat_out << us << " " << cnt << "\n";
            }
        }
    }

    // Compute throughput and write throughput file
    double elapsed_sec = ((double)(total_et.tv_sec - total_st.tv_sec)) + ((double)(total_et.tv_usec - total_st.tv_usec)) / 1e6;
    if (elapsed_sec <= 0.0) elapsed_sec = 1e-9; // guard divide-by-zero
    double tpt = (success_ops) / elapsed_sec;
    {
        std::ofstream tpt_out(throughput_out_path, std::ios::out | std::ios::trunc);
        tpt_out << success_ops << " " << tpt << "\n";
    }

    printf("Completed %llu ops in %.3f sec (%.2f ops/s)\n",
           (unsigned long long)success_ops, elapsed_sec, tpt);

    // Write CAS stats in same directory as throughput file
    {
        std::string tpt_path(throughput_out_path);
        size_t slash = tpt_path.find_last_of("/\\");
        std::string dir = (slash == std::string::npos) ? std::string("") : tpt_path.substr(0, slash + 1);
        std::string cas_path = dir + "cas_stats.csv";
        std::ofstream cas_out(cas_path.c_str(), std::ios::out | std::ios::trunc);
        cas_out << "attempted_ops,success_ops,failed_ops,failed_reads_no_match,failed_writes_pre_cas,failed_cas,retry_cas,failed_ops_due_to_cas\n";
        cas_out << attempted_ops << "," << success_ops << "," << failed_ops << ","
                << failed_reads_no_match << "," << failed_writes_pre_cas << ","
                << cas_fail_cnt << "," << cas_retry_cnt << "," << failed_ops_due_to_cas << "\n";
    }

    return 0;
}
