#!/usr/bin/env python3
import sys
import ipaddress

def parse_ipv6_address(ipv6_addr):
    """
    Parse an IPv6 address and extract specific bit ranges.

    Bit ranges:
    - 0 to 27 bits: cluster id (28 bits)
    - 28 to 39 bits: tor id (12 bits)
    - 40 to 51 bits: isolation id (12 bits)
    - 52 to 63 bits: interface id (12 bits)
    """

    # Parse the IPv6 address
    try:
        addr = ipaddress.IPv6Address(ipv6_addr)
    except ipaddress.AddressValueError as e:
        print(f"Invalid IPv6 address: {e}")
        return

    # Convert to integer (128 bits)
    addr_int = int(addr)

    # Extract bit ranges (from the most significant bits)
    # IPv6 is 128 bits, we're working with the first 64 bits

    # Shift right to get the upper 64 bits
    upper_64 = addr_int >> 64

    # Extract each field
    cluster_id = (upper_64 >> 36) & 0xFFFFFFF      # bits 0-27 (28 bits)
    tor_id = (upper_64 >> 24) & 0xFFF              # bits 28-39 (12 bits)
    isolation_id = (upper_64 >> 12) & 0xFFF        # bits 40-51 (12 bits)
    interface_id = upper_64 & 0xFFF                # bits 52-63 (12 bits)

    # Display results
    print(f"\nIPv6 Address: {ipv6_addr}")
    print(f"Integer representation: {addr_int}")
    print(f"\n{'='*60}")
    print(f"Cluster ID    (bits 0-27):  {cluster_id:8d} (0x{cluster_id:07x})")
    print(f"TOR ID        (bits 28-39): {tor_id:8d} (0x{tor_id:03x})")
    print(f"Isolation ID  (bits 40-51): {isolation_id:8d} (0x{isolation_id:03x})")
    print(f"Interface ID  (bits 52-63): {interface_id:8d} (0x{interface_id:03x})")
    print(f"{'='*60}")

    # Show binary representation of the first 64 bits for verification
    #binary_repr = format(upper_64, '064b')
    #print(f"\nBinary (first 64 bits):")
    #print(f"Cluster:    {binary_repr[0:28]}")
    #print(f"TOR:        {binary_repr[28:40]}")
    #print(f"Isolation:  {binary_repr[40:52]}")
    #print(f"Interface:  {binary_repr[52:64]}")

if __name__ == "__main__":
    # Get input from user
    #ipv6_input = input("Enter an IPv6 address: ")
    ipv6_input = sys.argv[1]
    parse_ipv6_address(ipv6_input)
