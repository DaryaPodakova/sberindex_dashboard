import socket
import logging

logger = logging.getLogger("db_tools")

def scan_postgres_ports(hosts=None, ports=None, timeout=2):
    """
    Scans given hosts and ports for accessible PostgreSQL services.

    Args:
        hosts (list): List of hostnames/IPs to scan.
        ports (list): List of ports to scan.
        timeout (int): Connection timeout in seconds.

    Returns:
        list of dict: Each dict contains host, port, and status (True/False).
    """
    if hosts is None:
        hosts = ["localhost", "127.0.0.1", "host.docker.internal"]
    if ports is None:
        ports = [5432, 5433]

    results = []
    for host in hosts:
        for port in ports:
            status = False
            try:
                with socket.create_connection((host, port), timeout=timeout):
                    status = True
                    logger.debug(f"Connection successful to {host}:{port}")
            except Exception as e:
                status = False
                logger.debug(f"Connection failed to {host}:{port}: {e}")
            results.append({"host": host, "port": port, "status": status})
    return results

# Example usage:
if __name__ == "__main__":
    scan_results = scan_postgres_ports()
    for res in scan_results:
        print(f"{'✅' if res['status'] else '❌'} {res['host']}:{res['port']}")