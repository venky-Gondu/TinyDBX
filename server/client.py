import socket

HOST = "127.0.0.1"
PORT = 5555

def recv_line(sock):
    """Receive bytes until a newline, return decoded string (no newline)."""
    buf = ""
    while True:
        data = sock.recv(4096)
        if not data:
            return None
        buf += data.decode("utf-8", errors="replace")
        if "\n" in buf:
            line, rest = buf.split("\n", 1)
            return line

def repl():
    with socket.create_connection((HOST, PORT)) as s:
        # Read welcome
        welcome = recv_line(s)
        if welcome:
            print(welcome)

        print("Connected to MiniDB. Type SQL commands terminated with ';'. Type 'exit;' to quit.")
        while True:
            lines = []
            while True:
                line = input("MiniDB> ").rstrip()
                if line == "":
                    continue
                if line.lower() in ("exit", "quit"):
                    s.sendall(b"exit;\n")
                    print("Exiting client.")
                    return
                lines.append(line)
                if line.endswith(";"):
                    break
            command = " ".join(lines).strip()
            if not command.endswith(";"):
                command += ";"

            try:
                s.sendall(command.encode("utf-8"))
            except BrokenPipeError:
                print("Connection lost.")
                return
            resp = recv_line(s)
            if resp is None:
                print("No response (server closed connection).")
                return
            print(resp)

if __name__ == "__main__":
    repl()
