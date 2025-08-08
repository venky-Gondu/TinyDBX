# main.py

from db_core.parser import Parser

def main():
    parser = Parser()
    print("🗄️  Mini SQL-like DB (type 'exit' or 'quit' to leave)")

    while True:
        print("\nEnter SQL command:")
        command = ""

        while True:
            line = input("db> ").strip()

            # Exit condition (early check)
            if line.lower() in ["exit", "exit;", "quit", "quit;"]:
                print("👋 Exiting...")
                return

            command += line + " "

            if line.endswith(";"):
                break

        command = command.strip()
        result = parser.route(command)
        print(result)

if __name__ == "__main__":
    main()