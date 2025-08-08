import socketserver
import json
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db_core.parser import Parser
HOST = "127.0.0.1"
PORT = 5555

class MiniDBRequestHandler(socketserver.BaseRequestHandler):
    
        def handle(self):
            parser = Parser()
            conn = self.request
            conn.settimeout(None)

            welcome = "MiniDB server ready. Send semicolon-terminated commands.\n"
            conn.sendall(welcome.encode("utf-8"))

            buffer = ""
            while True:
                try:
                    data = conn.recv(4096)
                except ConnectionError:
                    break

                if not data:
                    break

                buffer += data.decode("utf-8", errors="replace")

                while ";" in buffer:
                    idx = buffer.index(";")
                    raw_cmd = buffer[:idx + 1].strip()
                    buffer = buffer[idx + 1:]

                    if not raw_cmd:
                        continue

                    cmd_lower = raw_cmd.lower()
                    if cmd_lower in ("exit;", "quit;"):
                        conn.sendall(b"Goodbye\n")
                        return

                    try:
                        result = parser.route(raw_cmd)
                    except Exception as e:
                        result = {"error": f"Server error: {e}"}

                    if isinstance(result, (dict, list)):
                        out = json.dumps(result, ensure_ascii=False)
                    else:
                        out = str(result)

                    out += "\n"
                    try:
                        conn.sendall(out.encode("utf-8"))
                    except BrokenPipeError:
                        return

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True



if __name__ == "__main__":
    print(f"Starting MiniDB server on {HOST}:{PORT}")

    with ThreadedTCPServer((HOST,PORT), MiniDBRequestHandler) as server: 
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nShutting down server.")
            server.shutdown()
                




    
     
