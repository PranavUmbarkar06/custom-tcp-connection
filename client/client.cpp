#include "protocol.h"

struct WindowSlot {
    bool received = false;
    Packet pkt;
};

vector<WindowSlot> recv_window(WINDOW_SIZE);
unordered_set<int> received_seqs;

int expected_seq = 0;
int flow_control_mode = MODE_WINDOW_ADVERTISING;
bool transfer_complete = false;
bool is_paused_state = false;
bool fin_received = false;   // Set if the sender's FIN arrives while data is still being flushed
int peer_fin_seq = -1;
mutex recv_mutex;

// Mode 3: Packet Loss Simulation
bool drop_mode = false;        // true when Mode 3 is selected
int data_packet_count = 0;     // Counts every DATA packet that arrives (retransmissions included)
int dropped_count = 0;         // How many packets we threw away on purpose

int sockfd;
sockaddr_in client_addr;
socklen_t client_len = sizeof(client_addr);

ofstream output_file;

int calculate_cumulative_ack() {
    int cum_ack = expected_seq;
    while (recv_window[cum_ack % WINDOW_SIZE].received && 
           recv_window[cum_ack % WINDOW_SIZE].pkt.hdr.seq_num == cum_ack) {
        cum_ack++;
    }
    return cum_ack;
}

int count_buffered_slots() {
    int count = 0;
    for (int i = 0; i < WINDOW_SIZE; i++) {
        if (recv_window[i].received) count++;
    }
    return count;
}

void network_listener_thread() {
    while (!transfer_complete) {
        Packet incoming_pkt{};
        int bytes = recvfrom(sockfd, &incoming_pkt, sizeof(Packet), 0,
                             (struct sockaddr*)&client_addr, &client_len);

        if (bytes <= 0) continue;
        log_event("RECV", flag_name(incoming_pkt.hdr.flags), envelope_num(incoming_pkt.hdr));

        // FIN can arrive before the writer has flushed everything: remember it for passive_close()
        if (incoming_pkt.hdr.flags & FLAG_FIN) {
            lock_guard<mutex> lock(recv_mutex);
            fin_received = true;
            peer_fin_seq = incoming_pkt.hdr.seq_num;
            cout << "[Receiver] Received FIN SEQ: " << peer_fin_seq << " (still flushing buffer)" << endl;
            continue;
        }

        // Mode 1: Catch Zero-Window Probes and respond with current window state immediately
        if (incoming_pkt.hdr.flags & FLAG_PROBE) {
            lock_guard<mutex> lock(recv_mutex);
            int occupied = count_buffered_slots();
            int current_win = WINDOW_SIZE - occupied;

            cout << "[Receiver] Received FLAG_PROBE from sender. Replying with ACK (Current Win: " 
                 << current_win << ")" << endl;

            Header ack_hdr{};
            ack_hdr.flags = FLAG_ACK;
            ack_hdr.ack_num = calculate_cumulative_ack();
            ack_hdr.window_size = current_win;

            log_event("SEND", "ACK", ack_hdr.ack_num, current_win);
            sendto(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
            continue;
        }

        // Mode 3: Drop every 3rd DATA packet that reaches the receiver.
        // The sender never gets an ACK for it, so it has to recover with
        // Fast Retransmit (3 duplicate ACKs) or an RTO timeout.
        if (drop_mode) {
            data_packet_count++;
            if (data_packet_count % 3 == 0) {
                dropped_count++;
                cout << "[Packet Loss] Dropping packet #" << data_packet_count
                     << " (SEQ: " << incoming_pkt.hdr.seq_num << ")" << endl;
                log_event("DROP", "DATA", incoming_pkt.hdr.seq_num);
                continue;
            }
        }

        // Verify Checksum
        unsigned int calculated = calculate_checksum(incoming_pkt.payload, incoming_pkt.hdr.payload_len);
        if (calculated != incoming_pkt.hdr.checksum) {
            cout << "[Receiver] Checksum mismatch. Dropping SEQ: " << incoming_pkt.hdr.seq_num << endl;
            log_event("DROP", "DATA", incoming_pkt.hdr.seq_num);
            continue;
        }

        int seq = incoming_pkt.hdr.seq_num;

        {
            lock_guard<mutex> lock(recv_mutex);

            if (received_seqs.find(seq) == received_seqs.end()) {
                if (seq >= expected_seq && seq < expected_seq + WINDOW_SIZE) {
                    int slot_index = seq % WINDOW_SIZE;
                    recv_window[slot_index].pkt = incoming_pkt;
                    recv_window[slot_index].received = true;
                    received_seqs.insert(seq);
                    cout << "[Receiver] Received Packet SEQ: " << seq << endl;
                    log_event("STORE", "DATA", seq);
                }
            }

            int occupied = count_buffered_slots();
            int cum_ack = calculate_cumulative_ack();

            // Mode 2: Trigger explicit PAUSE signal when buffer hits 70% threshold
            if (flow_control_mode == MODE_PAUSE_RESUME && occupied >= 7 && !is_paused_state) {
                is_paused_state = true;
                Header pause_hdr{};
                pause_hdr.flags = FLAG_PAUSE;
                log_event("SEND", "PAUSE", 0);
                sendto(sockfd, &pause_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
                cout << "[Receiver] Buffer threshold reached (70%). Sent FLAG_PAUSE!" << endl;
            }

            // Send Cumulative ACK back to sender
            Header ack_hdr{};
            ack_hdr.flags = FLAG_ACK;
            ack_hdr.ack_num = cum_ack;
            ack_hdr.window_size = (flow_control_mode == MODE_WINDOW_ADVERTISING) ? (WINDOW_SIZE - occupied) : WINDOW_SIZE;
            log_event("SEND", "ACK", ack_hdr.ack_num, ack_hdr.window_size);

            sendto(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
            cout << "[Receiver] Sent Cumulative ACK: " << ack_hdr.ack_num 
                 << " (Available Win: " << ack_hdr.window_size << ")" << endl;
        }
    }
}

void file_writer_thread() {
    while (!transfer_complete) {
        {
            lock_guard<mutex> lock(recv_mutex);

            int slot_index = expected_seq % WINDOW_SIZE;

            if (recv_window[slot_index].received && recv_window[slot_index].pkt.hdr.seq_num == expected_seq) {
                Packet& current_pkt = recv_window[slot_index].pkt;

                // Simulated I/O latency to demonstrate flow control behavior
                this_thread::sleep_for(chrono::milliseconds(50));

                output_file.write(current_pkt.payload, current_pkt.hdr.payload_len);
                cout << "[Writer] Flushed SEQ " << expected_seq << " to disk." << endl;
                log_event("FLUSH", "DATA", expected_seq);

                if (current_pkt.hdr.flags & FLAG_EOF) {
                    cout << "[Writer] Received EOF flag. Transfer complete." << endl;
                    transfer_complete = true;
                }

                recv_window[slot_index].received = false;
                expected_seq++;

                int occupied = count_buffered_slots();

                // Mode 2: Trigger explicit RESUME signal when buffer drops below 20%
                if (flow_control_mode == MODE_PAUSE_RESUME && occupied <= 2 && is_paused_state) {
                    is_paused_state = false;
                    Header resume_hdr{};
                    resume_hdr.flags = FLAG_RESUME;
                    log_event("SEND", "RESUME", 0);
                    sendto(sockfd, &resume_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
                    cout << "[Receiver] Buffer clear (<=20%). Sent FLAG_RESUME!" << endl;
                }
            }
        }
        this_thread::sleep_for(chrono::milliseconds(5));
    }
}

// 4-Way Connection Termination (Receiver is the Passive Closer)
// ESTABLISHED -> CLOSE_WAIT -> LAST_ACK -> CLOSED
void passive_close() {
    string state = "ESTABLISHED";
    int my_fin_seq = 0;   // Receiver sent no data, so its FIN uses sequence number 0
    int retries = 0;

    // Step 1: Wait for the sender's FIN (the listener thread may have caught it already)
    while (!fin_received) {
        Packet incoming_pkt{};
        int bytes = recvfrom(sockfd, &incoming_pkt, sizeof(Packet), 0,
                             (struct sockaddr*)&client_addr, &client_len);
        if (bytes <= 0) continue;
        log_event("RECV", flag_name(incoming_pkt.hdr.flags), envelope_num(incoming_pkt.hdr));

        if (incoming_pkt.hdr.flags & FLAG_FIN) {
            fin_received = true;
            peer_fin_seq = incoming_pkt.hdr.seq_num;
            cout << "[Receiver] Received FIN SEQ: " << peer_fin_seq << endl;
        } else {
            // Our last data ACK was lost and the sender retransmitted: ACK it again
            Header ack_hdr{};
            ack_hdr.flags = FLAG_ACK;
            ack_hdr.ack_num = expected_seq;
            ack_hdr.window_size = WINDOW_SIZE;
            log_event("SEND", "ACK", expected_seq, WINDOW_SIZE);
            sendto(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
            cout << "[Receiver] Duplicate SEQ " << incoming_pkt.hdr.seq_num
                 << " after EOF. Re-sent Cumulative ACK: " << expected_seq << endl;
        }
    }

    // Step 2: ACK the sender's FIN
    Header ack_hdr{};
    ack_hdr.flags = FLAG_ACK;
    ack_hdr.ack_num = peer_fin_seq + 1;
    log_event("SEND", "ACK", ack_hdr.ack_num);
    sendto(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
    cout << "[Receiver] Sent ACK for FIN: " << ack_hdr.ack_num << endl;
    log_state("Receiver", state, "CLOSE_WAIT");

    // Application finishes up before closing its own side
    cout << "[Receiver] Application closing its side of the connection..." << endl;
    this_thread::sleep_for(chrono::milliseconds(500));

    // Step 3: Send our own FIN (with ACK set, like real TCP)
    Header fin_hdr{};
    fin_hdr.flags = FLAG_FIN | FLAG_ACK;
    fin_hdr.seq_num = my_fin_seq;
    fin_hdr.ack_num = peer_fin_seq + 1;
    log_event("SEND", "FIN", my_fin_seq);
    sendto(sockfd, &fin_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
    cout << "[Receiver] Sent FIN SEQ: " << my_fin_seq << endl;
    log_state("Receiver", state, "LAST_ACK");
    auto last_send_time = chrono::steady_clock::now();

    // Step 4: Wait for the final ACK, retransmitting our FIN on timeout
    while (state == "LAST_ACK") {
        auto now = chrono::steady_clock::now();
        auto duration = chrono::duration_cast<chrono::milliseconds>(now - last_send_time).count();
        if (duration > DEFAULT_RTO_MS) {
            if (++retries > MAX_FIN_RETRIES) {
                cout << "[Teardown] No ACK for FIN after " << MAX_FIN_RETRIES << " retries. Aborting close." << endl;
                break;
            }
            cout << "[RTO Timeout] Retransmitting FIN SEQ: " << my_fin_seq << endl;
            log_event("RESEND", "FIN", my_fin_seq);
            sendto(sockfd, &fin_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
            last_send_time = chrono::steady_clock::now();
        }

        Header in_hdr{};
        int bytes = recvfrom(sockfd, &in_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, &client_len);
        if (bytes <= 0) continue;
        log_event("RECV", flag_name(in_hdr.flags), envelope_num(in_hdr));

        if (in_hdr.flags & FLAG_FIN) {
            // Sender didn't get our ACK and retransmitted its FIN
            log_event("SEND", "ACK", ack_hdr.ack_num);
            sendto(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);
            cout << "[Receiver] Duplicate FIN received. Re-sent ACK: " << ack_hdr.ack_num << endl;
        } else if ((in_hdr.flags & FLAG_ACK) && in_hdr.ack_num == my_fin_seq + 1) {
            cout << "[Receiver] Received ACK for FIN: " << in_hdr.ack_num << endl;
            break;
        }
    }
    log_state("Receiver", state, "CLOSED");
}

int main() {
    cout << "Select Flow Control Mode:\n";
    cout << "1. Window Advertising Mode (with Probing)\n";
    cout << "2. Explicit PAUSE/RESUME Mode\n";
    cout << "3. Packet Loss Mode (drop every 3rd packet)\n";
    cout << "Choice: ";
    cin >> flow_control_mode;

    // Mode 3 keeps Window Advertising for flow control and turns on the drops
    if (flow_control_mode == MODE_PACKET_LOSS) {
        drop_mode = true;
        flow_control_mode = MODE_WINDOW_ADVERTISING;
        cout << "[Packet Loss] Every 3rd DATA packet will be dropped." << endl;
    }

    // Event log for the visualiser page
    vis_log.open("receiver_log.txt");

    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) return 1;

    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(PORT);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) return 1;

    cout << "Receiver listening on port " << PORT << "..." << endl;

    // 3-Way Handshake
    Header syn_hdr{};
    recvfrom(sockfd, &syn_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, &client_len);
    log_event("RECV", flag_name(syn_hdr.flags), envelope_num(syn_hdr));

    if (syn_hdr.flags & FLAG_SYN) {
        Header syn_ack_hdr{};
        syn_ack_hdr.flags = FLAG_SYN | FLAG_ACK;
        log_event("SEND", "SYNACK", 0);
        sendto(sockfd, &syn_ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, client_len);

        Header ack_hdr{};
        recvfrom(sockfd, &ack_hdr, sizeof(Header), 0, (struct sockaddr*)&client_addr, &client_len);
        log_event("RECV", flag_name(ack_hdr.flags), envelope_num(ack_hdr));
        cout << "[Handshake] Handshake Complete!\n" << endl;
        log_event("STATE", "ESTABLISHED");
    }

    // Lets the listener thread wake up and exit once transfer_complete is set
    set_recv_timeout(sockfd, RECV_POLL_MS);

    output_file.open("received_payload.xlsx", ios::binary);
    if (!output_file.is_open()) return 1;

    thread listener(network_listener_thread);
    thread writer(file_writer_thread);

    listener.join();
    writer.join();

    output_file.close();
    cout << "\n--- Saved to received_payload.xlsx ---" << endl;
    if (drop_mode) {
        cout << "[Packet Loss] Dropped " << dropped_count << " of " << data_packet_count
             << " DATA packets received." << endl;
    }

    cout << "\n--- Connection Teardown (4-Way) ---" << endl;
    passive_close();
    cout << "[Teardown] Connection Closed!" << endl;

    close(sockfd);
    vis_log.close();
    return 0;
}
