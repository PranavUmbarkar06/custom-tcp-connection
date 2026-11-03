import pyshark

def analyze_pcap(file_path, sender_ip, receiver_ip):
    # Open the pcapng file for offline analysis
    cap = pyshark.FileCapture(file_path)
    
    sent_count = 0
    received_count = 0
    
    print(f"Analyzing packets from: {file_path}\n")
    
    for packet in cap:
        try:
            # Check if the packet has IP layers
            if hasattr(packet, 'ip'):
                src = packet.ip.src
                dst = packet.ip.dst
                
                # Print packet summary or detailed features
                print(f"Packet: {packet.number} | {src} --> {dst} | Protocol: {packet.transport_layer}")
                
                # Count sent/received based on specified IPs
                if src == sender_ip and dst == receiver_ip:
                    sent_count += 1
                elif src == receiver_ip and dst == sender_ip:
                    received_count += 1
        except AttributeError:
            # Skip non-IP packets (e.g., ARP, LLMNR)
            continue

    cap.close()
    
    # Print summary at the end
    print("\n--- Summary ---")
    print(f"Packets sent from {sender_ip} to {receiver_ip}: {sent_count}")
    print(f"Packets received by {receiver_ip} from {sender_ip}: {received_count}")


analyze_pcap('cap1.pcapng', '127.0.0.1', '127.0.0.1')