import os
import sys
import socket
import threading
import time
import uuid
import json

from typing import Dict, List, Tuple, Optional


class Peer:
    # start a peer with networking, storage, and background threads
    def __init__(self, host="localhost", port=0, shared_dir="shared_files"):
        self.h = host
        self.p = port
        self.dir = shared_dir
        self.id = str(uuid.uuid4())[:4]
        self.peers = {}
        self.files = {}

        os.makedirs(shared_dir, exist_ok=True)

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((host, port))
        self.sock.listen(5)

        self.p = self.sock.getsockname()[1]

        print(f"Peer {self.id} started on {self.h}:{self.p}")

        self.run = True
        self.srv_thread = threading.Thread(target=self._handle_conn)
        self.brd_thread = threading.Thread(target=self._periodic_brd)

        self.srv_thread.daemon = True
        self.brd_thread.daemon = True

        self.srv_thread.start()
        self.brd_thread.start()

    # Accept incoming connections from other peers
    def _handle_conn(self):
        while self.run:
            try:
                client, addr = self.sock.accept()
                handler = threading.Thread(
                    target=self._handle_client, args=(client, addr)
                )
                handler.daemon = True
                handler.start()
            except Exception as e:
                if self.run:
                    print(f"Connection error: {e}")

    # Process messages from connected peers
    def _handle_client(self, client: socket.socket, addr: Tuple[str, int]):
        try:
            data = client.recv(1024)
            if not data:
                return

            msg_type = data[0:1].decode()

            if msg_type == "O":
                self._handle_offer(data[1:], client)

                peer_id = data[1:5].decode()
                if (
                    hasattr(self, "failed_connections")
                    and peer_id in self.failed_connections
                ):
                    del self.failed_connections[peer_id]

                flist = [
                    f
                    for f in os.listdir(self.dir)
                    if os.path.isfile(os.path.join(self.dir, f))
                ]
                fjson = json.dumps(flist)
                msg = f"O{self.id}{fjson}".encode()

                try:
                    client.sendall(msg)
                except Exception as e:
                    print(f"Error responding to peer: {e}")

            elif msg_type == "R":
                self._handle_req(data[1:], client)
            elif msg_type == "T":
                self._handle_transfer(data[1:], client)
            elif msg_type == "A":
                self._handle_ack(data[1:], client)
            else:
                print(f"error message type: {msg_type}")

        except Exception as e:
            print(f"Client communication error: {e}")
        finally:
            client.close()

    # Handle file list received from another peer
    def _handle_offer(self, data: bytes, client: socket.socket):
        try:
            pid = data[:4].decode()
            fdata = data[4:].decode()
            files = json.loads(fdata)

            caddr = client.getpeername()

            is_new_peer = pid not in self.peers
            has_new_files = pid not in self.files or set(files) != set(
                self.files.get(pid, [])
            )

            self.peers[pid] = caddr
            self.files[pid] = files

            if is_new_peer:
                print(f"New peer {pid} joined with {len(files)} files")
            elif has_new_files:
                print(f"Peer {pid} updated its file list: {files}")

        except Exception as e:
            if "json" in str(e).lower():
                print(f"Error parsing peer data: {e}")

    # Process file download requests from peers
    def _handle_req(self, data: bytes, client: socket.socket):
        try:
            fname = data.decode()
            fpath = os.path.join(self.dir, fname)

            if os.path.exists(fpath):
                self._send_file(fpath, client)
            else:
                client.sendall(b"E" + b"File not found")
                print(f"File not found: {fname}")
        except Exception as e:
            print(f"Request error: {e}")

    # Send a file in chunks to a requesting peer
    def _send_file(self, fpath: str, client: socket.socket):
        try:
            fname = os.path.basename(fpath)
            print(f"Sending {fname} to requesting peer...")

            with open(fpath, "rb") as f:
                csize = 1024
                cnum = 0
                total_chunks = 0

                while True:
                    chunk = f.read(csize)
                    if not chunk:
                        break

                    msg = b"T" + cnum.to_bytes(4, byteorder="big") + chunk
                    client.sendall(msg)

                    ack = client.recv(1024)
                    if not ack or ack[0:1] != b"A":
                        print(f"Error: No ack received for chunk {cnum}")
                        break

                    cnum += 1
                    total_chunks += 1

                client.sendall(b"T" + b"\xff\xff\xff\xff" + b"EOF")
                print(f"File {fname} sent successfully")
        except Exception as e:
            print(f"Error sending file: {e}")

    # done by _handle_req
    def _handle_transfer(self, data: bytes, client: socket.socket):
        pass

    def _handle_ack(self, data: bytes, client: socket.socket):
        pass

    # Regularly announce presence and file list to peers
    def _periodic_brd(self):
        while self.run:
            time.sleep(3)

            while self.run:
                try:
                    self.broadcast()
                except Exception as e:
                    print(f"Error in peer communication: {e}")
                time.sleep(20)

    # Send file list to all known peers and check connectivity
    def broadcast(self):
        flist = [
            f for f in os.listdir(self.dir) if os.path.isfile(os.path.join(self.dir, f))
        ]

        fjson = json.dumps(flist)
        msg = f"O{self.id}{fjson}".encode()

        peers_to_remove = []

        if self.peers:
            for pid, (h, p) in list(self.peers.items()):
                try:
                    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                        s.settimeout(5)
                        try:
                            s.connect((h, p))
                            s.sendall(msg)

                            try:
                                s.settimeout(2)
                                response = s.recv(1024)
                                if response and response[0:1].decode() == "O":
                                    pass
                            except socket.timeout:
                                pass
                        except (ConnectionRefusedError, socket.timeout) as conn_err:
                            peers_to_remove.append(pid)
                except Exception as e:
                    pass

            for pid in peers_to_remove:
                if pid in self.peers:
                    if (
                        hasattr(self, "failed_connections")
                        and pid in self.failed_connections
                    ):
                        self.failed_connections[pid] += 1
                        if self.failed_connections[pid] >= 3:
                            print(f"Peer {pid} is not reachable, removing")
                            del self.peers[pid]
                            if pid in self.files:
                                del self.files[pid]
                            if pid in self.failed_connections:
                                del self.failed_connections[pid]
                    else:
                        if not hasattr(self, "failed_connections"):
                            self.failed_connections = {}
                        self.failed_connections[pid] = 1

    # download a file from a specific peer or any peer that has it
    def request_file(self, fname: str, pid: Optional[str] = None) -> bool:
        target = None

        if pid:
            if pid in self.peers and fname in self.files.get(pid, []):
                target = (pid, self.peers[pid])
        else:
            for p, files in self.files.items():
                if fname in files:
                    target = (p, self.peers[p])
                    break

        if not target:
            print(f"No peer has {fname}")
            return False

        pid, (h, p) = target

        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((h, p))

                msg = f"R{fname}".encode()
                s.sendall(msg)

                outpath = os.path.join(self.dir, fname)
                with open(outpath, "wb") as f:
                    while True:
                        data = s.recv(1029)
                        if not data:
                            break

                        mtype = data[0:1].decode()

                        if mtype == "T":
                            cnum = int.from_bytes(data[1:5], byteorder="big")
                            cdata = data[5:]

                            if cnum == 0xFFFFFFFF and cdata == b"EOF":
                                break

                            f.write(cdata)

                            ack = f"A{self.id}".encode()
                            s.sendall(ack)

                        elif mtype == "E":
                            err = data[1:].decode()
                            print(f"Peer error: {err}")
                            return False

            print(f"Got file {fname} from peer {pid}")
            return True

        except Exception as e:
            print(f"Request error: {e}")
            return False

    # Connect to a new peer by address
    def connect(self, host: str, port: int):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect((host, port))

                flist = [
                    f
                    for f in os.listdir(self.dir)
                    if os.path.isfile(os.path.join(self.dir, f))
                ]
                fjson = json.dumps(flist)
                msg = f"O{self.id}{fjson}".encode()

                s.sendall(msg)

                try:
                    s.settimeout(5)
                    data = s.recv(1024)
                    if data and data[0:1].decode() == "O":
                        try:
                            peer_id = data[1:5].decode()
                            fdata = data[5:].decode()
                            files = json.loads(fdata)

                            self.peers[peer_id] = (host, port)
                            self.files[peer_id] = files
                            print(
                                f"Connected to peer {peer_id} at {host}:{port} ({len(files)} files available)"
                            )
                        except Exception as e:
                            print(f"Error processing peer response: {e}")
                except socket.timeout:
                    print(f"Connected to peer at {host}:{port} (no response received)")
        except Exception as e:
            print(f"Connection error: {e}")

    # shut down the peer
    def stop(self):
        self.run = False
        self.sock.close()
        print(f"Peer {self.id} stopped")


def main():
    host, port, shared_dir = "localhost", 0, "shared_files"
    # check and parse cmd line args
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("First argument must be a port number")
            sys.exit(1)
    if len(sys.argv) > 2:
        shared_dir = sys.argv[2]

    # directory name with port
    if port != 0:
        shared_dir = f"{shared_dir}_{port}"

    # create peer instance
    p = Peer(host=host, port=port, shared_dir=shared_dir)

    # menu
    try:
        while True:
            print("\n---------- P2P File Sharing System ----------")
            print("1. List local files")
            print("2. List peers and files")
            print("3. Request file")
            print("4. Connect to peer")
            print("5. Exit")
            print("--------------------------------------------")

            try:
                c = input("\nEnter choice: ")
            except EOFError:
                break
            # List local files
            if c == "1":
                files = os.listdir(p.dir)
                shared_files = [
                    f for f in files if os.path.isfile(os.path.join(p.dir, f))
                ]
                print("\nYour shared files:")
                if not shared_files:
                    print("  No files available")
                else:
                    for f in shared_files:
                        file_size = os.path.getsize(os.path.join(p.dir, f))
                        print(f"  {f} ({file_size} bytes)")
            # List peers and files
            elif c == "2":
                print("\nConnected peers and their files:")
                if not p.peers:
                    print("  No peers connected")
                else:
                    for pid, files in p.files.items():
                        addr = p.peers.get(pid, ("Unknown", "Unknown"))
                        print(f"  Peer {pid} at {addr[0]}:{addr[1]}:")
                        if not files:
                            print("    No files shared")
                        else:
                            for f in files:
                                print(f"    - {f}")
            # Request a file
            elif c == "3":
                fname = input("File name to download: ")
                pid = input("Enter peer ID, leave empty for all: ")
                if not pid:
                    pid = None
                p.request_file(fname, pid)
            # connect to a peer
            elif c == "4":
                h = input("Enter peer host: ")
                try:
                    port = int(input("Enter peer port: "))
                    p.connect(h, port)
                except ValueError:
                    print("Error: Port must be a number")
            # exit gracefully
            elif c == "5":
                break

            else:
                print("Invalid choice")

    except KeyboardInterrupt:
        print("\nShutting down peer...")

    finally:
        p.stop()


if __name__ == "__main__":
    main()
