#include "protocol.h"

// Shared Global Buffers & Window Tracking
vector<Packet> unacked_buffer;
int send_base = 0;
int next_seq_num = 0;
int user_rto_ms = DEFAULT_RTO_MS;
int flow_control_mode = MODE_WINDOW_ADVERTISING;

bool transfer_complete = false;
bool receiver_paused = false;
int current_remote_window = WINDOW_SIZE;
int dup_ack_count = 0;
int last_ack_num = -1;

mutex window_mutex;
int sockfd;
sockaddr_in receiver_addr;

// Thread 1: Chunker Thread
void chunker_thread_func(const string& filename) {
    ifstream file(filename, ios::binary);
    if (!file.is_open()) {
        cerr << "[Chunker] Error opening payload.xlsx file!" << endl;
        return;
    }

    int seq = 0;
    while (!file.eof()) {
        Packet pkt{};
        file.read(pkt.payload, BUFFER_SIZE);
        int bytes_read = file.gcount();

        if (bytes_read > 0) {
            pkt.hdr.seq_num = seq;
            pkt.hdr.ack_num = 0;
            pkt.hdr.flags = 0;
            pkt.hdr.payload_len = bytes_read;
            pkt.hdr.checksum = calculate_checksum(pkt.payload, bytes_read);

            if (file.peek() == EOF) {
                pkt.hdr.flags |= FLAG_EOF;
                cout << "[Chunker] Attached FLAG_EOF to packet sequence " << seq << endl;
            }

            {
                lock_guard<mutex> lock(window_mutex);
                unacked_buffer.push_back(pkt);
            }
            seq++;
        }
    }
    file.close();
    cout << "[Chunker] Finished chunking file into " << seq << " packets." << endl;
    log_event("TOTAL", "DATA", seq);
}

// Thread 2: Sender Thread (with Zero-Window Probing)
void sender_thread_func() {
    auto last_send_time = chrono::steady_clock::now();
    auto last_probe_time = chrono::steady_clock::now();

    while (!transfer_complete) {
        {
            lock_guard<mutex> lock(window_mutex);

            // Mode 2: Halt pushing data if explicitly paused
            if (flow_control_mode == MODE_PAUSE_RESUME && receiver_paused) {
                this_thread::sleep_for(chrono::milliseconds(10));
                continue;
            }

            // Mode 1: Zero-Window Probing Logic
            if (flow_control_mode == MODE_WINDOW_ADVERTISING && current_remote_window == 0) {
                auto now = chrono::steady_clock::now();
                auto probe_duration = chrono::duration_cast<chrono::milliseconds>(now - last_probe_time).count();

                if (probe_duration > 1000) { // Probe every 1 second
                    cout << "[Sender] Advertised Window is 0! Transmitting FLAG_PROBE..." << endl;
                    Packet probe_pkt{};
                    probe_pkt.hdr.flags = FLAG_PROBE;
                    probe_pkt.hdr.seq_num = next_seq_num;
                    log_event("SEND", "PROBE", next_seq_num);
                    sendto(sockfd, &probe_pkt, sizeof(Header), 0, 
                           (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
                    last_probe_time = chrono::steady_clock::now();
                }
            } else {
                // Transmit normal data packets within active window
                int active_window = (flow_control_mode == MODE_WINDOW_ADVERTISING) ? current_remote_window : WINDOW_SIZE;

                while (next_seq_num < send_base + active_window && next_seq_num < (int)unacked_buffer.size()) {
                    Packet pkt = unacked_buffer[next_seq_num];
                    log_event("SEND", "DATA", pkt.hdr.seq_num, active_window);
                    sendto(sockfd, &pkt, sizeof(Header) + pkt.hdr.payload_len, 0, 
                           (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
                    cout << "[Sender] Transmitted Packet SEQ: " << pkt.hdr.seq_num << endl;
                    next_seq_num++;
                    last_send_time = chrono::steady_clock::now();
                }
            }

            // Check for Timeout (RTO)
            auto now = chrono::steady_clock::now();
            auto duration = chrono::duration_cast<chrono::milliseconds>(now - last_send_time).count();
            if (send_base < next_seq_num && duration > user_rto_ms) {
                cout << "[RTO Timeout] Retransmitting base packet SEQ: " << send_base << endl;
                Packet pkt = unacked_buffer[send_base];
                log_event("RESEND", "DATA", send_base);
                sendto(sockfd, &pkt, sizeof(Header) + pkt.hdr.payload_len, 0, 
                       (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
                last_send_time = chrono::steady_clock::now();
            }

            if (send_base >= (int)unacked_buffer.size() && unacked_buffer.size() > 0) {
                transfer_complete = true;
            }
        }
        this_thread::sleep_for(chrono::milliseconds(5));
    }
}

// Thread 3: ACK & Control Frame Listener
void ack_listener_thread_func() {
    Header ack_hdr{};
    sockaddr_in from_addr{};
    socklen_t addr_len = sizeof(from_addr);

    while (!transfer_complete) {
        int bytes = recvfrom(sockfd, &ack_hdr, sizeof(Header), 0, 
                             (struct sockaddr*)&from_addr, &addr_len);
        if (bytes <= 0) continue;
        log_event("RECV", flag_name(ack_hdr.flags), envelope_num(ack_hdr), ack_hdr.window_size);

        lock_guard<mutex> lock(window_mutex);

        // Handle PAUSE/RESUME frames (Mode 2)
        if (ack_hdr.flags & FLAG_PAUSE) {
            receiver_paused = true;
            cout << "[Sender] Received FLAG_PAUSE! Halting packet output." << endl;
            continue;
        } else if (ack_hdr.flags & FLAG_RESUME) {
            receiver_paused = false;
            cout << "[Sender] Received FLAG_RESUME! Resuming packet output." << endl;
            continue;
        }

        // Handle ACKs & Window Updates (Mode 1 & standard ACK flow)
        if (ack_hdr.flags & FLAG_ACK) {
            current_remote_window = ack_hdr.window_size;

            if (ack_hdr.ack_num > send_base) {
                send_base = ack_hdr.ack_num;
                dup_ack_count = 0;
                last_ack_num = ack_hdr.ack_num;
                cout << "[ACK Listener] Cumulative ACK: " << ack_hdr.ack_num 
                     << " (Advertised Win: " << current_remote_window << ")" << endl;
            } else if (ack_hdr.ack_num == last_ack_num) {
                dup_ack_count++;
                if (dup_ack_count == 3 && send_base < (int)unacked_buffer.size()) {
                    cout << "[Fast Retransmit] 3 Dup ACKs! Resending SEQ: " << send_base << endl;
                    Packet pkt = unacked_buffer[send_base];
                    log_event("RESEND", "DATA", send_base);
                    sendto(sockfd, &pkt, sizeof(Header) + pkt.hdr.payload_len, 0, 
                           (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
                    dup_ack_count = 0;
                }
            }
            log_event("BASE", "ACK", send_base, current_remote_window);
        }
    }
}

// 4-Way Connection Termination (Sender is the Active Closer)
// ESTABLISHED -> FIN_WAIT_1 -> FIN_WAIT_2 -> TIME_WAIT -> CLOSED
void active_close() {
    string state = "ESTABLISHED";
    int fin_seq = (int)unacked_buffer.size();   // FIN takes the next sequence number after the data
    int retries = 0;

    Header in_hdr{};
    sockaddr_in from_addr{};
    socklen_t addr_len = sizeof(from_addr);

    // Step 1: Send FIN
    Packet fin_pkt{};
    fin_pkt.hdr.flags = FLAG_FIN;
    fin_pkt.hdr.seq_num = fin_seq;
    log_event("SEND", "FIN", fin_seq);
    sendto(sockfd, &fin_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
    cout << "[Sender] Transmitted FIN SEQ: " << fin_seq << endl;
    log_state("Sender", state, "FIN_WAIT_1");
    auto last_send_time = chrono::steady_clock::now();

    // Steps 2 & 3: Wait for ACK of our FIN, then for the receiver's FIN
    while (state != "TIME_WAIT") {
        auto now = chrono::steady_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(now - last_send_time).count();
        if (state == "FIN_WAIT_1" && duration > user_rto_ms) {
            if (++retries > MAX_FIN_RETRIES) {
                cout << "[Teardown] No ACK for FIN after " << MAX_FIN_RETRIES << " retries. Aborting close." << endl;
                log_state("Sender", state, "CLOSED");
                return;
            }
            cout << "[RTO Timeout] Retransmitting FIN SEQ: " << fin_seq << endl;
            log_event("RESEND", "FIN", fin_seq);
            sendto(sockfd, &fin_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
            last_send_time = chrono::steady_clock::now();
        }

        int bytes = recvfrom(sockfd, &in_hdr, sizeof(Header), 0, (struct sockaddr*)&from_addr, &addr_len);
        if (bytes <= 0) continue;
        log_event("RECV", flag_name(in_hdr.flags), envelope_num(in_hdr));

        if (state == "FIN_WAIT_1" && (in_hdr.flags & FLAG_ACK) && in_hdr.ack_num == fin_seq + 1) {
            cout << "[ACK Listener] Received ACK for FIN: " << in_hdr.ack_num << endl;
            log_state("Sender", state, "FIN_WAIT_2");
        }

        if (state == "FIN_WAIT_2" && (in_hdr.flags & FLAG_FIN)) {
            cout << "[ACK Listener] Received FIN SEQ: " << in_hdr.seq_num << " from receiver" << endl;
            Packet ack_pkt{};
            ack_pkt.hdr.flags = FLAG_ACK;
            ack_pkt.hdr.ack_num = in_hdr.seq_num + 1;
            log_event("SEND", "ACK", ack_pkt.hdr.ack_num);
            sendto(sockfd, &ack_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
            cout << "[Sender] Transmitted ACK for FIN: " << ack_pkt.hdr.ack_num << endl;
            log_state("Sender", state, "TIME_WAIT");
        }
    }

    // Step 4: TIME_WAIT for 2*MSL, re-ACKing the receiver's FIN in case our last ACK was lost
    cout << "[Teardown] Waiting 2*MSL (" << 2 * MSL_MS << " ms) in TIME_WAIT..." << endl;
    auto time_wait_start = chrono::steady_clock::now();
    while (chrono::duration_cast<chrono::milliseconds>(chrono::steady_clock::now() - time_wait_start).count() < 2 * MSL_MS) {
        int bytes = recvfrom(sockfd, &in_hdr, sizeof(Header), 0, (struct sockaddr*)&from_addr, &addr_len);
        if (bytes > 0) log_event("RECV", flag_name(in_hdr.flags), envelope_num(in_hdr));
        if (bytes > 0 && (in_hdr.flags & FLAG_FIN)) {
            Packet ack_pkt{};
            ack_pkt.hdr.flags = FLAG_ACK;
            ack_pkt.hdr.ack_num = in_hdr.seq_num + 1;
            log_event("SEND", "ACK", ack_pkt.hdr.ack_num);
            sendto(sockfd, &ack_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
            cout << "[TIME_WAIT] Duplicate FIN received. Re-sent ACK: " << ack_pkt.hdr.ack_num << endl;
        }
    }
    log_state("Sender", state, "CLOSED");
}

int main() {
    cout << "Select Flow Control Mode:\n";
    cout << "1. Window Advertising Mode (with Probing)\n";
    cout << "2. Explicit PAUSE/RESUME Mode\n";
    cout << "3. Packet Loss Mode (receiver drops every 3rd packet)\n";
    cout << "Choice: ";
    cin >> flow_control_mode;

    // In Mode 3 the sender works like Mode 1; the receiver does the dropping
    if (flow_control_mode == MODE_PACKET_LOSS) {
        flow_control_mode = MODE_WINDOW_ADVERTISING;
    }

    cout << "Enter RTO Value in ms (default 500): ";
    cin >> user_rto_ms;

    // Event log for the visualiser page
    vis_log.open("sender_log.txt");

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        cerr << "Failed to create socket!" << endl;
        return 1;
    }

    memset(&receiver_addr, 0, sizeof(receiver_addr));
    receiver_addr.sin_family = AF_INET;
    receiver_addr.sin_port = htons(PORT);
    inet_pton(AF_INET, "127.0.0.1", &receiver_addr.sin_addr);

    // 3-Way Handshake
    cout << "\n--- Initiating Handshake ---" << endl;
    Packet syn_pkt{};
    syn_pkt.hdr.flags = FLAG_SYN;
    log_event("SEND", "SYN", 0);
    sendto(sockfd, &syn_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));

    Header ack_hdr{};
    sockaddr_in temp_addr{};
    socklen_t addr_len = sizeof(temp_addr);
    recvfrom(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&temp_addr, &addr_len);
    log_event("RECV", flag_name(ack_hdr.flags), envelope_num(ack_hdr));

    if ((ack_hdr.flags & (FLAG_SYN | FLAG_ACK)) == (FLAG_SYN | FLAG_ACK)) {
        Packet ack_pkt{};
        ack_pkt.hdr.flags = FLAG_ACK;
        log_event("SEND", "ACK", 0);
        sendto(sockfd, &ack_pkt, sizeof(Header), 0, (struct sockaddr*)&receiver_addr, sizeof(receiver_addr));
        cout << "[Handshake] Connection Established!\n" << endl;
        log_event("STATE", "ESTABLISHED");
    }

    // Lets the ACK listener wake up and exit once transfer_complete is set
    set_recv_timeout(sockfd, RECV_POLL_MS);

    thread chunker(chunker_thread_func, "payload.xlsx");
    thread sender(sender_thread_func);
    thread ack_listener(ack_listener_thread_func);

    chunker.join();
    sender.join();
    ack_listener.join();

    cout << "\n--- Transfer Complete ---" << endl;

    cout << "\n--- Initiating Connection Teardown (4-Way) ---" << endl;
    active_close();
    cout << "[Teardown] Connection Closed!" << endl;

    close(sockfd);
    vis_log.close();
    return 0;
}
