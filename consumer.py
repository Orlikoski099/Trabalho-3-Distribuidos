import Pyro5.api


def main():
    líder = Pyro5.api.Proxy("PYRONAME:Lider-Epoca1")
    while True:
        command = input(
            "Digite 'log' para buscar o log ou 'sair' para terminar: ")
        if command.lower() == "sair":
            break
        elif command.lower() == "log":
            offset = input("Digite o offset: ")
            logL = líder.get_log(offset)
            print("Log do líder:", logL)


if __name__ == "__main__":
    main()
