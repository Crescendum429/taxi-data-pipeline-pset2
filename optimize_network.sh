#!/bin/bash
# Network optimization script for maximum throughput

echo "Optimizing network for maximum performance..."

# Disable WiFi to force Ethernet usage
echo "Prioritizing Ethernet over WiFi..."
sudo ip route del default via 192.168.100.1 dev wlp0s20f3 2>/dev/null

# Network buffer optimizations (apply immediately)
echo "Optimizing network buffers..."
echo 'net.core.rmem_default = 262144' | sudo tee -a /etc/sysctl.conf
echo 'net.core.rmem_max = 16777216' | sudo tee -a /etc/sysctl.conf
echo 'net.core.wmem_default = 262144' | sudo tee -a /etc/sysctl.conf
echo 'net.core.wmem_max = 16777216' | sudo tee -a /etc/sysctl.conf
echo 'net.core.netdev_max_backlog = 5000' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_window_scaling = 1' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_rmem = 4096 65536 16777216' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_wmem = 4096 65536 16777216' | sudo tee -a /etc/sysctl.conf
echo 'net.ipv4.tcp_congestion_control = bbr' | sudo tee -a /etc/sysctl.conf

# Apply settings immediately
sudo sysctl -p

# Increase container priority
echo "Optimizing container priority..."
sudo renice -10 $(pgrep -f "docker.*pset2_scheduler") 2>/dev/null

# CPU governor to performance mode
echo "Setting CPU to performance mode..."
echo performance | sudo tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null

# Disable unnecessary services temporarily
echo "Optimizing system resources..."
sudo systemctl stop bluetooth.service 2>/dev/null
sudo systemctl stop cups.service 2>/dev/null

echo "Network optimization completed!"
echo "Ethernet interface: enx5c531028247d (100Mb/s)"
echo "Latency to internet: ~17ms"
echo "Ready for high-throughput data transfer"